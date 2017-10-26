import json
import responses
import pytest
from quartic.common.services import Service, Howl, Catalogue
from quartic.common.exceptions import QuarticException


API_ROOT = "http://quartic/api/"

def _url(partial):
    return API_ROOT + partial


class TestService:
    class DummyService(Service):
        def __init__(self):
            Service.__init__(self, API_ROOT, "my-token")

        def invoke(self, method, allow_404=False):
            lookup = {
                "HEAD": self._head,
                "GET": self._get,
                "POST": self._post,
                "PUT": self._put,
                "DELETE": self._delete,
            }
            return lookup[method]("stuff", allow_404)

    service = DummyService()

    params = [
        ("HEAD"),
        ("GET"),
        ("POST"),
        ("PUT"),
        ("DELETE"),
    ]

    @pytest.mark.parametrize("method", params)
    @responses.activate
    def test_adds_auth_header(self, method):
        responses.add(method, _url("stuff"))

        self.service.invoke(method)

        assert responses.calls[0].request.headers["Authorization"] == "Bearer my-token"

    @pytest.mark.parametrize("method", params)
    @responses.activate
    def test_raises_exception_for_non_2xx(self, method):
        responses.add(method, _url("stuff"), status=300)

        with pytest.raises(QuarticException):
            self.service.invoke(method)

    @pytest.mark.parametrize("method", params)
    @responses.activate
    def test_raises_exception_for_404(self, method):
        responses.add(method, _url("stuff"), status=404)

        with pytest.raises(QuarticException):
            self.service.invoke(method)

    @pytest.mark.parametrize("method", params)
    @responses.activate
    def test_returns_none_for_404_if_allowed(self, method):
        responses.add(method, _url("stuff"), status=404)

        assert self.service.invoke(method, allow_404=True) is None


class TestHowl:
    howl = Howl(api_root=API_ROOT, bearer_token="my-token")

    @responses.activate
    def test_encodes_paths_correctly(self):
        assert self.howl.path("yeah") == "/yeah/managed/yeah"
        assert self.howl.path("yeah", "but-no") == "/yeah/managed/yeah/but-no"
        assert self.howl.path("yeah", "but/no") == "/yeah/managed/yeah/but%2Fno"

    @responses.activate
    def test_gets_key_existence(self):
        responses.add(responses.HEAD, _url("yeah/managed/yeah/key"))

        assert self.howl.exists_path(self.howl.path("yeah", "key")) is True

    @responses.activate
    def test_gets_key_nonexistence(self):
        responses.add(responses.HEAD, _url("yeah/managed/yeah/key"), status=404)

        assert self.howl.exists_path(self.howl.path("yeah", "key")) is False

    @responses.activate
    def test_gets_text_content(self):
        responses.add(responses.GET, _url("yeah/managed/yeah/key"), body="Hello world!")

        r = self.howl.download_path(self.howl.path("yeah", "key"))
        assert r.text == "Hello world!"

    @responses.activate
    def test_puts_content_if_key_specified(self):
        responses.add(responses.PUT, _url("yeah/managed/yeah/key"))

        r = self.howl.upload_path(data="Hello world!", path=self.howl.path("yeah", "key"))

        r.raise_for_status()
        assert responses.calls[0].request.body == "Hello world!"

    @responses.activate
    def test_posts_content_if_key_unspecified(self):
        responses.add(responses.POST, _url("yeah/managed/yeah"))

        r = self.howl.upload_path(data="Hello world!", path=self.howl.path("yeah"))

        r.raise_for_status()
        assert responses.calls[0].request.body == "Hello world!"


class TestCatalogue:
    catalogue = Catalogue(api_root=API_ROOT, bearer_token="my-token")

    @responses.activate
    def test_gets_all_datasets(self):
        datasets = {"a": "b", "c": "d"}
        responses.add(responses.GET, _url("datasets"), json=datasets)

        assert self.catalogue.datasets() == datasets

    @responses.activate
    def test_gets_dataset(self):
        dataset = {"a": "b", "c": "d"}
        responses.add(responses.GET, _url("datasets/my-namespace/dataset-id"), json=dataset)

        assert self.catalogue.get("my-namespace", "dataset-id") == dataset

    @responses.activate
    def test_encodes_dataset_id_correctly(self):
        dataset = {"a": "b", "c": "d"}
        responses.add(responses.GET, _url("datasets/my-namespace/dataset%2Fid"), json=dataset)

        assert self.catalogue.get("my-namespace", "dataset/id") == dataset

    @responses.activate
    def test_returns_none_if_dataset_non_existent(self):
        responses.add(responses.GET, _url("datasets/my-namespace/dataset-id"), status=404)

        assert self.catalogue.get("my-namespace", "dataset-id") is None

    @responses.activate
    def test_puts_anonymous_dataset(self):
        dataset = {"a": "b", "c": "d"}
        coords = {"id": 123}
        responses.add(responses.POST, _url("datasets/my-namespace"), json=coords)

        assert self.catalogue.put("my-namespace", None, dataset) == coords
        assert json.loads(responses.calls[0].request.body) == dataset

    @responses.activate
    def test_puts_dataset(self):
        dataset = {"a": "b", "c": "d"}
        coords = {"id": 123}
        responses.add(responses.GET, _url("datasets/my-namespace/dataset-id"), status=404)
        responses.add(responses.PUT, _url("datasets/my-namespace/dataset-id"), json=coords)

        assert self.catalogue.put("my-namespace", "dataset-id", dataset) == coords
        assert json.loads(responses.calls[1].request.body) == dataset

    @responses.activate
    def test_puts_dataset_fails_if_overwrite_would_modify(self):
        old_dataset = {"a": "x", "c": "d", "metadata": {"registered": 1234}}
        dataset = {"a": "b", "c": "d"}
        responses.add(responses.GET, _url("datasets/my-namespace/dataset-id"), json=old_dataset)

        with pytest.raises(QuarticException):
            self.catalogue.put("my-namespace", "dataset-id", dataset)

    @responses.activate
    def test_puts_dataset_succeeds_if_overwrite_will_not_modify(self):
        old_dataset = {"a": "b", "c": "d", "metadata": {"registered": 1234}}
        dataset = {"a": "b", "c": "d", "metadata": {}}
        coords = {"id": 123}
        responses.add(responses.GET, _url("datasets/my-namespace/dataset-id"), json=old_dataset)
        responses.add(responses.PUT, _url("datasets/my-namespace/dataset-id"), json=coords)

        self.catalogue.put("my-namespace", "dataset-id", dataset)

        assert json.loads(responses.calls[1].request.body) == dataset

    @responses.activate
    def test_puts_dataset_without_checking_old_if_overwrite_flag_set(self):
        dataset = {"a": "b", "c": "d"}
        coords = {"id": 123}
        responses.add(responses.PUT, _url("datasets/my-namespace/dataset-id"), json=coords)

        self.catalogue.put("my-namespace", "dataset-id", dataset, overwrite=True)

        assert json.loads(responses.calls[0].request.body) == dataset

    @responses.activate
    def test_unregisters_dataset(self):
        responses.add(responses.DELETE, _url("datasets/my-namespace/dataset-id"))

        self.catalogue.unregister("my-namespace", "dataset-id")
