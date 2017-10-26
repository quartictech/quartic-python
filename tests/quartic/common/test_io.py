import json
import csv
from quartic.common.io import DownloadFile, UploadFile, DatasetReader, DatasetWriter, LocalIoFactory
import pandas as pd
import pytest
from mock import MagicMock, Mock, call, ANY
import pyarrow as pa
import pyarrow.parquet as pq


class TestDownloadFile:
    def test_concatenates_streamed_chunks_and_closes_response(self):
        path = "/some/thing"
        response = MagicMock()
        response.__enter__.return_value.iter_content.return_value = iter([
            bytes([0, 1, 2, 3]),
            bytes([10, 11, 12, 13]),
            bytes([20, 21, 22, 23])
        ])
        howl = Mock()
        howl.download_path.return_value = response

        df = DownloadFile(howl, path)

        assert bytes([0, 1, 2, 3, 10, 11, 12, 13, 20, 21, 22, 23]) == df.read()
        howl.download_path.assert_called_with(path, stream=True)
        response.__exit__.assert_called_with(ANY, ANY, ANY)


class TestUploadFile:
    # TODO - test other aspects of UploadFile

    def test_uploads_on_close(self):
        path = "/some/thing"

        howl = Mock()

        uf = UploadFile(howl, path, "w+b")

        uf.close()

        howl.upload_path.assert_called_with(path=path, data=ANY)


class TestDatasetReader:
    def test_reads_raw(self, tmpdir):
        path = tmpdir.join("input.bin")
        io_factory = LocalIoFactory(path)

        data = bytes([0, 1, 2, 3, 255])
        with open(path, "wb") as f:
            f.write(data)

        data_read = DatasetReader(io_factory).raw().read()

        assert data == data_read


    def test_reads_csv(self, tmpdir):
        path = tmpdir.join("input.csv")
        io_factory = LocalIoFactory(path)

        with open(path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=["name", "age"])
            writer.writeheader()
            writer.writerow({"name": "Alice Foo", "age": 25})
            writer.writerow({"name": "Bob Bar", "age": 36})
            writer.writerow({"name": "Charlie Baz", "age": 49})

        df_read = DatasetReader(io_factory).csv()

        pd.util.testing.assert_frame_equal(
            pd.DataFrame.from_items([
                ("name", ["Alice Foo", "Bob Bar", "Charlie Baz"]),
                ("age", [25, 36, 49])
            ]),
            df_read
        )


    def test_reads_csv_with_non_ascii_chars(self, tmpdir):
        path = tmpdir.join("input.csv")
        io_factory = LocalIoFactory(path)

        with open(path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=["item", "price"])
            writer.writeheader()
            writer.writerow({"item": "baguette", "price": "€10"})

        df_read = DatasetReader(io_factory).csv()

        pd.util.testing.assert_frame_equal(
            pd.DataFrame.from_items([
                ("item", ["baguette"]),
                ("price", ["€10"])
            ]),
            df_read
        )


    def test_reads_parquet(self, tmpdir):
        path = tmpdir.join("input.pq")
        io_factory = LocalIoFactory(path)

        df = pd.DataFrame({
            "foo": [0, 1, 2],
            "bar": ["hello", "goodbye", "oh dear"]
        })
        pq.write_table(pa.Table.from_pandas(df), path.strpath)

        df_read = DatasetReader(io_factory).parquet()

        pd.util.testing.assert_frame_equal(df, df_read)


    # TODO - test parquet handling of timestamps?


    def test_reads_json(self, tmpdir):
        path = tmpdir.join("input.json")
        io_factory = LocalIoFactory(path)

        data = {
            "foo": [0, 1, 2],
            "bar": {
                "hello": 5,
                "goodbye": True,
                "oh dear": None
            }
        }

        with open(path, "w") as f:
            json.dump(data, f)

        data_read = DatasetReader(io_factory).json()

        assert data == data_read


    def test_reads_json_with_non_ascii_chars(self, tmpdir):
        path = tmpdir.join("input.json")
        io_factory = LocalIoFactory(path)

        data = {"foo": "€10"}

        with open(path, "w") as f:
            json.dump(data, f, ensure_ascii=False)  # See https://stackoverflow.com/a/14870531/129570

        data_read = DatasetReader(io_factory).json()

        assert data == data_read


class TestDatasetWriter:
    def test_passes_extensions_to_callback(self, tmpdir):
        path = tmpdir.join("output.pq")
        io_factory = LocalIoFactory(path)

        extensions = {}

        on_close = Mock()

        with DatasetWriter(io_factory, on_close, extensions) as f:
            f.extensions()["foo"] = "bar"

        assert on_close.mock_calls == [call({"foo": "bar"})]


    def test_parquet_succeeds(self, tmpdir):
        path = tmpdir.join("output.pq")
        io_factory = LocalIoFactory(path)

        df = pd.DataFrame({
            "foo": [0, 1, 2],
            "bar": ["hello", "goodbye", "oh dear"]
        })

        with DatasetWriter(io_factory, lambda x: None, None) as f:
            f.parquet(df)

        df_written = pq.read_table(path.strpath).to_pandas()

        pd.util.testing.assert_frame_equal(df, df_written)


    def test_parquet_with_mixed_type_columns(self, tmpdir):
        path = tmpdir.join("output.pq")
        io_factory = LocalIoFactory(path)

        df = pd.DataFrame({
            "foo": [0, "string", 2],     # Mixed type!
            "bar": [4, 5, 6],
        })

        with pytest.raises(ValueError) as excinfo:
            with DatasetWriter(io_factory, lambda x: None, None) as f:
                f.parquet(df)

        assert "bar" not in str(excinfo.value)
        assert "foo" in str(excinfo.value)


    def test_parquet_mixed_ignores_none(self, tmpdir):
        path = tmpdir.join("output.pq")
        io_factory = LocalIoFactory(path)

        df = pd.DataFrame({
            "wat": [None, "la", "dispute"],
            "yep": [1, 2, 3]
        })

        with DatasetWriter(io_factory, lambda x: None, None) as f:
            f.parquet(df)
