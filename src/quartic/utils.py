import tempfile
import requests


def raise_if_invalid_coord(*coords):
    for coord in coords:
        if coord and ":" in coord:
            raise ValueError("Dataset coordinate cannot contain ':' character")


class QuarticException(Exception):
    pass


class DownloadFile:
    def __init__(self, url):
        self._tmp = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
        r = requests.get(url, stream=True)
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                self._tmp.write(chunk)
        self._tmp.seek(0)
    def read(self, *args, **kwargs):
        return self._tmp.read(*args, **kwargs)
    def seek(self, *args, **kwargs):
        return self._tmp.seek(*args, **kwargs)
    def tell(self, *args, **kwargs):
        return self._tmp.tell(*args, **kwargs)
    def close(self):
        self._tmp.close()
    def __enter__(self):
        return self
    def __exit__(self, *args, **kwargs):
        self.close()


class UploadFile:
    def __init__(self, url, method, mode='w+b'):
        self._url = url
        self._tmp = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024, mode=mode)
        self._method = method
        self.response = None

    def write(self, *args, **kwargs):
        return self._tmp.write(*args, **kwargs)
    def seek(self, *args, **kwargs):
        return self._tmp.seek(*args, **kwargs)
    def tell(self, *args, **kwargs):
        return self._tmp.tell(*args, **kwargs)
    def close(self):
        self._tmp.flush()
        self._tmp.seek(0)
        if self._method == "PUT":
            self.response = requests.put(self._url, data=self._tmp)
        elif self._method == "POST":
            self.response = requests.post(self._url, data=self._tmp)
        self.response.raise_for_status()
        self._tmp.close()
    def cancel(self):
        self._tmp.close()

    def __enter__(self):
        return self
    def __exit__(self, *args, **kwargs):
        self.close()


class LocalIoFactory:
    def __init__(self, path):
        self._path = path
    def writable_file(self, mode="w+b"):
        file = open(self._path, mode)
        file.cancel = lambda: None
        return file

    def readable_file(self, mode="r+b"):
        return open(self._path, mode)

class RemoteIoFactory:
    def __init__(self, url, method):
        self._url = url
        self._method = method

    def writable_file(self, mode="w+b"):
        return UploadFile(self._url, self._method, mode)

    def readable_file(self, mode="r+b"):
        return DownloadFile(self._url)
