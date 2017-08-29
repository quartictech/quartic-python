from quartic.common.io import DatasetWriter, LocalIoFactory
import pandas as pd
import pytest
from mock import Mock, call
import pyarrow.parquet as pq


# TODO - test DatasetReader


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


    def test_parquet_reports_columns_with_all_none(self, tmpdir):
        path = tmpdir.join("output.pq")
        io_factory = LocalIoFactory(path)

        df = pd.DataFrame({
            "foo": [0, None, 2],
            "bar": [None, None, None]
        })

        with pytest.raises(ValueError) as excinfo:
            with DatasetWriter(io_factory, lambda x: None, None) as f:
                f.parquet(df)

        assert "bar" in str(excinfo.value)
        assert "foo" not in str(excinfo.value)


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
