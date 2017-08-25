import requests

from .io import DatasetWriter, DatasetReader, RemoteIoFactory
from quartic.common.dataset import raise_if_invalid_coord
from .services import Howl, Catalogue
from .exceptions import QuarticException

class Quartic:
    def __init__(self, url_format="http://localhost:{port}/api/", shell=None):
        self._catalogue = Catalogue(url_format.format(service="catalogue", port=8090))
        self._howl = Howl(url_format.format(service="howl", port=8120))
        self.shell = shell

    def __call__(self, namespace):
        return Namespace(self._catalogue, self._howl, namespace, self._notebook_name())

    def _notebook_name(self):
        # WEIRD WEIRD HACK
        try:
            conn_file = self.shell.config['IPKernelApp']['connection_file'].split('/')[-1]
            kernel_id = conn_file.split(".")[0].split("-", 1)[1]
            notebooks = requests.get("http://localhost:8888/analysis/api/sessions").json()
            return [nb for nb in notebooks if nb["kernel"]["id"] == kernel_id][0]['path']
        except:
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

    def catalogue_entry_exists(self):
        return self._get_dataset() is not None

    def data_exists(self):
        dataset = self._get_dataset()
        if not dataset:
            return False
        return self._howl.exists_path(dataset["locator"]["path"])

    def metadata(self):
        return self._get_dataset()["metadata"]

    def extensions(self):
        return self._get_dataset()["extensions"]

    def register_raw(self, partial_path, name, description=None,
                     mime_type="application/octet-stream", attribution="quartic",
                     extensions=None, streaming=False, overwrite=False):
        self._assert_raw_dataset_id()

        path = "raw/" + partial_path

        if not self._howl.exists(self._namespace, path):
            raise QuarticException("Data cannot be found at path: {}".format(path))
        if description is None:
            description = name

        dataset = self._dataset_config(name, description, attribution, extensions,
                                       path, streaming, mime_type)
        self._put_dataset(dataset, overwrite)
        return self

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

        url = self._howl.path_url(dataset["locator"]["path"])
        return DatasetReader(self._io_factory_class(url, None))

    def writer(self, name=None, description=None, mime_type="application/octet-stream",
               attribution="quartic", extensions=None, streaming=False):
        self._assert_non_raw_dataset_id()

        dataset = self._get_dataset()
        if dataset:
            return self._writer_for_existing_dataset(name, description, dataset)
        else:
            return self._writer_for_new_dataset(name, description, mime_type,
                                                attribution, extensions, streaming)

    def _writer_for_new_dataset(self, name, description, mime_type, attribution,
                                extensions, streaming):
        if name is None:
            raise QuarticException("New datasets must specify name")
        if description is None:
            description = name

        def on_close(final_extensions):
            dataset = self._dataset_config(name, description, attribution,
                                           final_extensions, self._dataset_id, streaming, mime_type)
            r = self._put_dataset(dataset)
            if not self._dataset_id:
                self._dataset_id = r["id"]

        url = self._howl.url(self._namespace, self._dataset_id)
        method = "PUT" if self._dataset_id else "POST"
        return DatasetWriter(self._io_factory_class(url, method), on_close, extensions)

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

        url = self._howl.path_url(dataset["locator"]["path"])
        return DatasetWriter(self._io_factory_class(url, "PUT"), on_close, dataset["extensions"])

    def _get_dataset(self):
        if self._dataset_id is None:
            return None
        return self._catalogue.get(self._namespace, self._dataset_id)

    def _put_dataset(self, dataset, overwrite=False):
        return self._catalogue.put(self._namespace, self._dataset_id, dataset, overwrite)

    def _assert_raw_dataset_id(self):
        if not self._dataset_id or not self._dataset_id.startswith("raw/"):
            raise QuarticException("Dataset ID must begin with raw/")

    def _assert_non_raw_dataset_id(self):
        if self._dataset_id:
            if self._dataset_id.startswith("raw/"):
                raise QuarticException("Dataset ID must not begin with raw/")
            if self._dataset_id == "raw":
                raise QuarticException("Dataset ID must not be raw")

    def _dataset_config(self, name, description, attribution, extensions, partial_path,
                        streaming, mime_type):
        return {
            "metadata": {
                "name": name,
                "description": description,
                "attribution": attribution
            },
            "extensions": self._enrich_extensions(extensions),
            "locator": {
                "type": "cloud",
                "path": self._howl.path(self._namespace, partial_path),
                "streaming": streaming,
                "mimeType": mime_type
            }
        }

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
