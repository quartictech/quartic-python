import urllib.parse
from datadiff import diff
import requests
from requests.auth import AuthBase
from quartic import __version__
from .exceptions import QuarticException


USER_AGENT = "quartic-python/{}".format(__version__)


class BearerAuth(AuthBase):
    def __init__(self, token):
        self._token = token

    def __call__(self, r):
        r.headers["Authorization"] = "Bearer {}".format(self._token)
        return r


def get_session(bearer_token):
    session = requests.Session()
    session.auth = BearerAuth(bearer_token)
    session.headers["User-Agent"] = USER_AGENT
    return session


class Service:
    """Abstract class for wrapping a service API."""
    def __init__(self, api_root, bearer_token):
        self._api_root = api_root
        self._session = get_session(bearer_token)

    def _head(self, resource, allow_404=False, **kwargs):
        return self._check(self._session.head(self._url(resource), **kwargs), allow_404=allow_404)

    def _get(self, resource, allow_404=False, **kwargs):
        return self._check(self._session.get(self._url(resource), **kwargs), allow_404=allow_404)

    def _post(self, resource, allow_404=False, **kwargs):
        return self._check(self._session.post(self._url(resource), **kwargs), allow_404=allow_404)

    def _put(self, resource, allow_404=False, **kwargs):
        return self._check(self._session.put(self._url(resource), **kwargs), allow_404=allow_404)

    def _delete(self, resource, allow_404=False, **kwargs):
        return self._check(self._session.delete(self._url(resource), **kwargs), allow_404=allow_404)

    def _url(self, resource):
        if resource.startswith("/"):
            resource = resource.lstrip("/")
        return urllib.parse.urljoin(self._api_root, resource)

    @staticmethod
    def _quote(s):
        return urllib.parse.quote(s, safe="")

    @staticmethod
    def _check(r, allow_404=False):
        if 200 <= r.status_code < 300:
            return r
        elif r.status_code == 404 and allow_404:
            return None
        else:
            raise QuarticException(r.text)


class Howl(Service):
    def __init__(self, api_root, bearer_token):
        Service.__init__(self, api_root, bearer_token)

    def exists_path(self, path):
        return self._head(path, allow_404=True) is not None

    def download_path(self, path, stream=False):
        return self._get(path, stream=stream)

    def upload_path(self, path, data):
        num_bits = len(path.split("/"))
        # Recall that the path includes a leading slash, so counts are one higher
        if num_bits == 5:
            return self._put(path, data=data)
        elif num_bits == 4:
            return self._post(path, data=data)
        else:
            raise QuarticException("Malformed Howl path: {}".format(path))

    @staticmethod
    def path(namespace, key=None):
        if key is None:
            return "/{}/managed/{}".format(namespace, namespace)
        else:
            return "/{}/managed/{}/{}".format(namespace, namespace, Service._quote(key))


class Catalogue(Service):
    def __init__(self, api_root, bearer_token):
        Service.__init__(self, api_root, bearer_token)

    def datasets(self):
        r = self._get("/datasets")
        if r:
            return r.json()

    def get(self, namespace, dataset_id):
        r = self._get(self._path(namespace, dataset_id), allow_404=True)
        if r:
            return r.json()

    def put(self, namespace, dataset_id, dataset, overwrite=False):
        if dataset_id is None:
            return self._post(self._path(namespace, dataset_id), json=dataset).json()
        else:
            if not overwrite:
                existing = self.get(namespace, dataset_id)
                if existing:
                    existing["metadata"].pop("registered")

                    if existing != dataset:
                        msg = "[{} / {}] catalogue entries differ:\n{}.\n Specify overwrite=True to replace".format(
                            namespace,
                            dataset_id,
                            diff(existing, dataset)
                        )
                        raise QuarticException(msg)

            return self._put(self._path(namespace, dataset_id), json=dataset).json()

    def unregister(self, namespace, dataset_id):
        return self._delete(self._path(namespace, dataset_id))

    @staticmethod
    def _path(namespace, dataset_id):
        if dataset_id is None:
            return "/datasets/{}".format(namespace)
        else:
            return "/datasets/{}/{}".format(namespace, Service._quote(dataset_id))
