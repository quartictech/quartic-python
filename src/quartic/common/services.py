import urllib.parse
from datadiff import diff
from .exceptions import QuarticException
from .utils import get_session, get_opener

class Service:
    """Abstract class for wrapping a service API."""
    def __init__(self, api_root):
        self._api_root = api_root
        self._session = get_session()
        self._opener = get_opener()

    def _head(self, resource, **kwargs):
        return self._check(self._session.head(self._url(resource), **kwargs))

    def _get(self, resource, **kwargs):
        return self._check(self._session.get(self._url(resource), **kwargs), allow_404=True)

    def _post(self, resource, **kwargs):
        return self._check(self._session.post(self._url(resource), **kwargs))

    def _put(self, resource, **kwargs):
        return self._check(self._session.put(self._url(resource), **kwargs))

    def _delete(self, resource, **kwargs):
        return self._check(self._session.delete(self._url(resource), **kwargs))

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
    def __init__(self, api_root):
        Service.__init__(self, api_root)

    def exists(self, namespace, path):
        """Check the existence of the specified file, returning True or False accordingly."""
        try:
            self._head(self.path(namespace, path))
            return True
        except QuarticException:
            # TODO - be more discriminating about 4xx codes vs. any old error
            return False

    def exists_path(self, locator_path):
        """Check the exists of the specified file at preformed path."""
        return self._head(locator_path)

    def download(self, namespace, path):
        """Download file."""
        return self._get(self.path(namespace, path))

    def upload(self, data, namespace, path=None):
        if path:
            return self._put(self.path(namespace, path), data=data)
        else:
            r = self._post(namespace, data=data)
        return r

    def path(self, namespace, path):
        if path is None:
            return namespace
        return "/{}/managed/{}/{}".format(namespace, namespace, self._quote(path))

    def _unmanaged_path(self, namespace, path):
        return "/{}/unmanaged/{}".format(namespace, self._quote(path))

    def unmanaged_open(self, namespace, path):
        url = self._url(self._unmanaged_path(namespace, path))
        try:
            return self._opener.open(url)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise QuarticException("Unmanaged dataset not found: {} (namespace = {})".format(path, namespace))
            raise

    def url(self, namespace, path):
        return self._url(self.path(namespace, path))

    def path_url(self, full_path):
        return self._url(full_path)


class Catalogue(Service):
    def __init__(self, api_root):
        Service.__init__(self, api_root)

    def datasets(self):
        return self._get("datasets")

    def get(self, namespace, dataset_id):
        r = self._get(self.url(namespace, dataset_id))
        if r:
            return r.json()

    def put(self, namespace, dataset_id, dataset, overwrite=False):
        if dataset_id is None:
            return self._post(self.url(namespace, dataset_id), json=dataset).json()
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

            self._put(self.url(namespace, dataset_id), json=dataset)

    def unregister(self, namespace, dataset_id):
        return self._delete(self.url(namespace, dataset_id))

    @staticmethod
    def url(namespace, dataset_id):
        if dataset_id is None:
            return "datasets/{}".format(namespace)
        else:
            return "datasets/{}/{}".format(namespace, Service._quote(dataset_id))
