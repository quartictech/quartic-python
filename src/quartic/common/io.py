import re
import tempfile
import warnings
import json
from .log import logger
log = logger(__name__)


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


def _warn_if_any_all_nan_columns(df):
    nan_columns = df.columns.drop(df.dropna(how="all", axis=1).columns).values
    if nan_columns.any():
        log.warning("Columns with all-NaN values: %s", nan_columns)


def _raise_if_any_mixed_type_columns(df):
    bad_columns = [c for c in df.columns if len(set([type(x) for x in df[c].unique() if x is not None])) > 1]
    if bad_columns:
        raise ValueError("Cannot write columns with mixed types: {}".format(bad_columns))


def _write_parquet(df, f):
    import pyarrow.parquet as pq
    import pyarrow as pa

    # Because pyarrow can't handle these (with a cryptic error)
    _warn_if_any_all_nan_columns(df)
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


class DatasetReader:
    def __init__(self, io_factory):
        self._io_factory = io_factory

    def raw(self):
        return self._io_factory.readable_file()

    def csv(self, *args, **kwargs):
        return _read_csv(self.raw(), *args, **kwargs)

    def parquet(self):
        return _read_parquet(self.raw())

    def json(self):
        return json.load(self.raw())


class DatasetWriter:
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
        self._reopen_as_text_file()
        json.dump(o, self._file)

    def csv(self, df, *args, **kwargs):
        self._reopen_as_text_file()
        df.to_csv(self._file, *args, **kwargs)

    # Bit of a hack to deal with the fact that json.dump wants a text file
    # TODO - see if https://stackoverflow.com/a/14870531/129570 solves this issue
    def _reopen_as_text_file(self):
        self._file.cancel()
        self._file = self._io_factory.writable_file(mode="w+")


class DownloadFile:
    def __init__(self, howl, path):
        self._tmp = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
        with howl.download_path(path, stream=True) as r:
            for chunk in r.iter_content(chunk_size=1024):
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
    def __init__(self, howl, path, mode):
        self._howl = howl
        self._path = path
        self._tmp = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024, mode=mode)
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
        self.response = self._howl.upload_path(path=self._path, data=self._tmp)
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

    def readable_file(self):
        return open(self._path, "r+b")


class RemoteIoFactory:
    def __init__(self, howl, path):
        self._howl = howl
        self._path = path

    def writable_file(self, mode="w+b"):
        return UploadFile(self._howl, self._path, mode)

    def readable_file(self):
        return DownloadFile(self._howl, self._path)
