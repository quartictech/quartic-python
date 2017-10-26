from quartic.common.quartic import Quartic, Dataset
import pytest
from mock import Mock, call

# TODO - test reader
# TODO - test delete



class TestBasics:
    def test_dataset_syntax(self):
        q = Quartic(api_token="abcd", url_format="http://localhost:{port}/api/")
        dataset = q("foo").dataset("bar")

        assert repr(dataset) == "foo::bar"


    def test_invalid_coords(self):
        q = Quartic(api_token="abcd", url_format="http://localhost:{port}/api/")

        with pytest.raises(ValueError):
            q("foo:x")
        with pytest.raises(ValueError):
            q("foo").dataset("bar:x")


class TestWriter:
    # TODO - test ordering (i.e. on_close)
    # TODO - test rewrite
    # TODO - test dataset_id is captured

    def setup_method(self, method):
        # pylint: disable=W0201
        self.catalogue = mock_catalogue()
        self.howl = mock_howl()
        self.io_factory_class = mock_io_factory_class()


    def test_invokes_howl_for_new_datasets(self):
        dataset = Dataset(self.catalogue, self.howl, "yeah", None,
                          io_factory_class=self.io_factory_class)

        with dataset.writer("foo", "bar"):
            pass

        assert self.io_factory_class.mock_calls[0] == call(self.howl, "/some-path")


    def test_invokes_howl_for_new_datasets_if_id_specified(self):
        dataset = Dataset(self.catalogue, self.howl, "yeah", "1234",
                          io_factory_class=self.io_factory_class)

        with dataset.writer("foo", "bar"):
            pass

        assert self.io_factory_class.mock_calls[0] == call(self.howl, "/some-path")


    def test_creates_catalogue_entry(self):
        dataset = Dataset(self.catalogue, self.howl, "yeah", None,
                          io_factory_class=self.io_factory_class)

        with dataset.writer("foo", "bar"):
            pass

        assert self.catalogue.mock_calls == [call.put("yeah", None, {
            "metadata": {
                "name": "foo",
                "description": "bar",
                "attribution": "quartic"
            },
            "extensions": {
            },
            "locator": {
                "type": "cloud",
                "path": "/some-path",
                "streaming": False,
                "mime_type": "application/octet-stream"
            }
        }, False)]


    def test_uses_name_as_description_if_no_description_supplied(self):
        dataset = Dataset(self.catalogue, self.howl, "yeah", None,
                          io_factory_class=self.io_factory_class)

        with dataset.writer("foo"):   # No decription
            pass

        assert self.catalogue.put.mock_calls[0][1][2]["metadata"]["description"] == "foo"


def mock_catalogue():
    mock = Mock()
    mock.get.return_value = None
    mock.put.return_value = {"id": "1234"}
    return mock

def mock_howl():
    mock = Mock()
    mock.path.return_value = "/some-path"
    mock.url.return_value = "http://some-url"
    return mock

def mock_io_factory_class():
    mock = Mock()
    mock.return_value = Mock()
    return mock
