import re
import tempfile
import warnings
import urllib.request
import json
import requests

# Adapted from:
# http://stackoverflow.com/questions/28682562/pandas-read-csv-converting-mixed-types-columns-as-string
def _read_csv(*args, coerce_mixed_types=True, target_type=str, **kwargs):
    """ Read a CSV file using pandas but forcing 'mixed-type' columns to target_type """
    import pandas
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")

        df = pandas.read_csv(*args, **kwargs)
        # We have an error on specific columns, try and load them as string
        if coerce_mixed_types:
            for w in ws:
                s = str(w.message)
                print("Warning message:", s)
                match = re.search(r"Columns \(([0-9,]+)\) have mixed types\.", s)
                if match:
                    columns = match.group(1).split(",") # Get columns as a list
                    columns = [int(c) for c in columns]
                    print("Applying %s dtype to columns:" % target_type, columns)
                    df.iloc[:, columns] = df.iloc[:, columns].astype(target_type)
        return df

def _raise_if_any_all_nan_columns(df):
    nan_columns = df.columns.drop(df.dropna(how="all", axis=1).columns).values
    if nan_columns:
        raise ValueError("Cannot write columns with all-NaN values: {}".format(nan_columns))

def _raise_if_any_mixed_type_columns(df):
    bad_columns = [c for c in df.columns if len(set([type(x) for x in df[c].unique() if x is not None])) > 1]
    if bad_columns:
        raise ValueError("Cannot write columns with mixed types: {}".format(bad_columns))


def _write_parquet(df, f):
    import pyarrow.parquet as pq
    import pyarrow as pa

    # Because pyarrow can't handle these (with a cryptic error)
    _raise_if_any_all_nan_columns(df)
    _raise_if_any_mixed_type_columns(df)

    # we have to coerce timestamps to millisecond resolution as
    # nanoseconds are not yet supported by Arrow/Parquet
    tbl = pa.Table.from_pandas(df, timestamps_to_ms=True)
    pq.write_table(tbl, pa.PythonFile(f))

def _read_parquet(f):
    import pyarrow.parquet as pq
    with f:
        tbl = pq.read_table(f)
        return tbl.to_pandas()

class DatasetReader(object):
    def __init__(self, io_factory):
        self._io_factory = io_factory

    def csv(self, *args, **kwargs):
        return _read_csv(self._io_factory.url(), *args, **kwargs)    # TODO - assumption about url() here!

    def parquet(self):
        return _read_parquet(self._io_factory.readable_file())

    def raw(self):
        return requests.get(self._io_factory.url())    # TODO - assumption about url() here!

    def json(self):
        # TODO: switch to using json.load() once decoding issues figured out
        return self.raw().json()

class DatasetWriter(object):
    def __init__(self, io_factory, on_close, catalogue_extensions):
        self._io_factory = io_factory
        self._on_close = on_close
        self._extensions = catalogue_extensions
        self._file = None

    def __enter__(self):
        self._file = self._io_factory.writable_file()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if exception_type:
            self._file.cancel()
        else:
            self._file.close()
            self._on_close(self._extensions)

    def write(self, *args, **kwargs):
        self._file.write(*args, **kwargs)

    def extensions(self):
        return self._extensions

    def parquet(self, df, *args, **kwargs):
        return _write_parquet(df, self)

    def json(self, o):
        # Bit of a hack to deal with the fact that json.dump wants a text file
        self._file.cancel()
        self._file = self._io_factory.writable_file(mode="w+")
        json.dump(o, self._file)

    def csv(self, df, *args, **kwargs):
        self._file.cancel()
        self._file = self._io_factory.writable_file(mode="w+")
        df.to_csv(self._file, *args, **kwargs)


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
    def __init__(self, url, method, mode="w+b"):
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

    def url(self):
        return self._url
