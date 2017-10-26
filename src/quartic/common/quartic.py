# pylint: disable=too-many-arguments
import requests

from quartic.common.dataset import raise_if_invalid_coord
from .io import DatasetWriter, DatasetReader, RemoteIoFactory
from .services import Howl, Catalogue
from .exceptions import QuarticException

class Quartic:
    def __init__(self, api_token, url_format="http://localhost:{port}/api/", shell=None):
        self._catalogue = Catalogue(url_format.format(service="catalogue", port=8090), bearer_token=api_token)
        self._howl = Howl(url_format.format(service="howl", port=8120), bearer_token=api_token)
        self._shell = shell

    def __call__(self, namespace):
        return Namespace(self._catalogue, self._howl, namespace, self._notebook_name())

    def _notebook_name(self):
        # WEIRD WEIRD HACK
        try:
            conn_file = self._shell.config["IPKernelApp"]["connection_file"].split("/")[-1]
            kernel_id = conn_file.split(".")[0].split("-", 1)[1]
            notebooks = requests.get("http://localhost:8888/analysis/api/sessions").json()
            return [nb for nb in notebooks if nb["kernel"]["id"] == kernel_id][0]["path"]
        except: # pylint: disable=bare-except
            return None


class Namespace:
    def __init__(self, catalogue, howl, namespace, notebook_name):
        raise_if_invalid_coord(namespace)
        self._catalogue = catalogue
        self._howl = howl
        self._namespace = namespace
        self._notebook_name = notebook_name

    def dataset(self, dataset_id):
        return Dataset(self._catalogue, self._howl, self._namespace,
                       dataset_id, self._notebook_name)


class Dataset:
    def __init__(self, catalogue, howl, namespace, dataset_id,
                 notebook_name=None, io_factory_class=RemoteIoFactory):
        raise_if_invalid_coord(dataset_id)
        self._catalogue = catalogue
        self._howl = howl
        self._namespace = namespace
        self._dataset_id = dataset_id
        self._notebook_name = notebook_name
        self._io_factory_class = io_factory_class # Used for testing

    def metadata(self):
        return self._get_dataset()["metadata"]

    def extensions(self):
        return self._get_dataset()["extensions"]

    def update(self, metadata=None, extensions=None):
        if metadata is None and extensions is None:
            raise QuarticException("Must specify metadata or extensions")
        dataset = self._get_dataset()

        # Doesn't make a lot of sense otherwise
        if dataset is None:
            raise QuarticException("Dataset does not exist: {}".format(self))

        dataset["metadata"].pop("registered")
        if metadata is not None:
            dataset["metadata"] = metadata
        if extensions is not None:
            dataset["extensions"] = extensions
        self._put_dataset(dataset, overwrite=True)

    def delete(self):
        assert self._dataset_id, "Uncreated anonymous datasets cannot be deleted"
        self._catalogue.unregister(self._namespace, self._dataset_id)

    def reader(self):
        dataset = self._get_dataset()
        if not dataset:
            raise QuarticException("Can't read non-existent dataset: {}".format(self))
        return DatasetReader(self._io_factory_class(self._howl, dataset["locator"]["path"], None))

    def writer(self, name=None, description=None, mime_type="application/octet-stream",
               attribution="quartic", extensions=None, streaming=False):
        dataset = self._get_dataset()
        if dataset:
            return self._writer_for_existing_dataset(name, description, dataset)
        else:
            return self._writer_for_new_dataset(name, description, mime_type, attribution, extensions, streaming)

    def _writer_for_new_dataset(self, name, description, mime_type, attribution, extensions, streaming):
        if name is None:
            raise QuarticException("New datasets must specify name")
        if description is None:
            description = name

        howl_path = self._howl.path(self._namespace, self._dataset_id)

        def on_close(final_extensions):
            dataset = {
                "metadata": {
                    "name": name,
                    "description": description,
                    "attribution": attribution
                },
                "extensions": self._enrich_extensions(final_extensions),
                "locator": {
                    "type": "cloud",
                    "path": howl_path,
                    "streaming": streaming,
                    "mime_type": mime_type
                }
            }
            r = self._put_dataset(dataset)
            if not self._dataset_id:
                self._dataset_id = r["id"]

        return DatasetWriter(self._io_factory_class(self._howl, howl_path), on_close, extensions)

    def _writer_for_existing_dataset(self, name, description, dataset):
        assert dataset["locator"]["type"] == "cloud"

        def on_close(final_extensions):
            dataset["extensions"] = final_extensions
            dataset["metadata"].pop("registered")
            if name is not None:
                dataset["metadata"]["name"] = name
            if description is not None:
                dataset["metadata"]["description"] = description
            self._put_dataset(dataset, True)

        return DatasetWriter(
            self._io_factory_class(self._howl, dataset["locator"]["path"]),
            on_close,
            dataset["extensions"])

    def _get_dataset(self):
        if self._dataset_id is None:
            return None
        return self._catalogue.get(self._namespace, self._dataset_id)

    def _put_dataset(self, dataset, overwrite=False):
        return self._catalogue.put(self._namespace, self._dataset_id, dataset, overwrite)

    def _enrich_extensions(self, extensions):
        out = {}
        if self._notebook_name:
            out["notebook"] = self._notebook_name
        if extensions:
            out.update(extensions)
        return out

    def __repr__(self):
        return "{namespace}::{dataset_id}".format(
            namespace=self._namespace,
            dataset_id=self._dataset_id)
