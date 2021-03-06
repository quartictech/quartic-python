import json
import os.path
import pytest
from mock import Mock, MagicMock
from quartic.common.exceptions import QuarticException, UserCodeExecutionException
from quartic import step
from quartic.dsl.node import Node
from quartic.common.dataset import Dataset, writer
from quartic.pipeline.runner.cli import main, parse_args
from quartic.dsl.context import DslContext

resources_dir = "tests/resources"

class TestCli:
    def test_evaluate_good_dag(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, os.path.join(resources_dir, "good_dag")])
        main(args)
        nodes = json.load(open(output_path))["nodes"]
        assert len(nodes) == 2

        nodes.sort(key=lambda x: x["info"]["name"])
        assert nodes[0]["info"] == {
            "name": "step1",
            "description": "First step",
            "file": os.path.join(resources_dir, "good_dag", "good_dag.py"),
            "line_range": [11, 14]
        }

        assert nodes[1]["info"] == {
            "name": "step2",
            "description": "Second step",
            "file": os.path.join(resources_dir, "good_dag", "good_dag.py"),
            "line_range": [16, 19]
        }


    def test_evaluate_bad_dag(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, os.path.join(resources_dir, "bad_dag")])
        with pytest.raises(UserCodeExecutionException):
            main(args)

    def test_evaluate_imports(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, os.path.join(resources_dir, "import_dag")])
        main(args)
        steps = json.load(open(output_path))
        assert len(steps) == 1

    def test_evaluate_disjoint_dag(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, os.path.join(resources_dir, "disjoint_dag")])
        main(args)
        nodes = json.load(open(output_path))["nodes"]
        assert len(nodes) == 2

        nodes.sort(key=lambda x: x["info"]["name"])
        assert nodes[0]["info"] == {
            "name": "step1",
            "description": "A description",
            "file": os.path.join(resources_dir, "disjoint_dag", "disjoint_dag.py"),
            "line_range": [11, 14]
        }
        assert nodes[1]["info"] == {
            "name": "step2",
            "description": "Another description",
            "file": os.path.join(resources_dir, "disjoint_dag", "disjoint_dag.py"),
            "line_range": [16, 19]
        }


    def test_execute_step(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, os.path.join(resources_dir, "good_dag")])
        main(args)

        nodes = json.load(open(output_path))["nodes"]
        args = parse_args([
            "--execute", nodes[0]["id"],
            "--namespace", "test",
            "--api-token", "my-special-token",
            os.path.join(resources_dir, "good_dag")])
        main(args)


class TestDataset:
    def test_init(self):
        assert Dataset(42) == Dataset("42")
        assert Dataset(Dataset("foo", "bar")) == Dataset("foo", "bar")    # Needs to be idempotent!


    def test_representation(self):
        assert repr(Dataset("foo")) == "::foo"
        assert repr(Dataset("foo", "bar")) == "bar::foo"


    def test_parse(self):
        assert Dataset.parse("::foo") == Dataset("foo")
        assert Dataset.parse("bar::foo") == Dataset("foo", "bar")


    def test_reject_invalid(self):
        with pytest.raises(ValueError):
            Dataset("foo:x")
        with pytest.raises(ValueError):
            Dataset("foo", "bar:x")


    def test_fully_qualified(self):
        assert Dataset("foo").fully_qualified("bar") == Dataset("foo", "bar")
        assert Dataset("foo", "baz").fully_qualified("bar") == Dataset("foo", "baz")


    def test_resolve(self):
        namespace = Mock()
        quartic = Mock()
        quartic.return_value = namespace

        Dataset("foo", "bar").resolve(quartic)

        quartic.assert_called_with("bar")
        namespace.dataset.assert_called_with("foo")


    def test_fail_to_resolve_unqualified(self):
        with pytest.raises(QuarticException):
            Dataset("foo").resolve(Mock())


    def test_equality(self):
        self._test_tuples(lambda d: d)


    def test_hash(self):
        self._test_tuples(hash)


    def _test_tuples(self, xform):
        for t in self._tuples:
            if t[2]:
                assert xform(t[0]) == xform(t[1])
            else:
                assert xform(t[0]) != xform(t[1])


    _tuples = [
        (Dataset("foo"), Dataset("foo"), True),
        (Dataset("foo"), Dataset("bar"), False),

        (Dataset("foo", "bar"), Dataset("foo", "bar"), True),
        (Dataset("foo", "bar"), Dataset("foo", "baz"), False),
        (Dataset("foo", "bar"), Dataset("goo", "bar"), False),

        (Dataset("foo"), Dataset("foo", "bar"), False),
    ]


class TestWriter:
    def test_basics(self):
        dataset = MagicMock()
        writer("foo", "bar").apply(dataset)

        dataset.writer.assert_called_with("foo", "bar")

    def test_parquet(self):
        dataset = MagicMock()
        writer("foo", "bar").parquet(42).apply(dataset)

        dataset.writer.return_value.__enter__.return_value.parquet.assert_called_with(42)


    def test_json(self):
        dataset = MagicMock()
        writer("foo", "bar").json(42).apply(dataset)

        dataset.writer.return_value.__enter__.return_value.json.assert_called_with(42)



class TestStep:
    def setup_method(self, method):
        # pylint: disable=attribute-defined-outside-init
        self.writer = Mock()
        def func(x: "foo", y: {"a": "bar", "b": "bear"}) -> "baz":
            self.x = x
            self.y = y
            return self.writer
        with DslContext():
            self.valid_step = step(func)


    def test_decorator_applies_node_wrapper(self):
        with DslContext():
            @step
            def func() -> "alice/bob":
                pass

        assert isinstance(func, Node)
        assert func.to_dict()["type"] == "step"


    def test_complains_if_unannotated_arguments(self):
        def func(x):
            pass

        with pytest.raises(QuarticException) as excinfo:
            step(func)

        assert "Unannotated argument" in str(excinfo.value)
        assert "'x'" in str(excinfo.value)


    def test_complains_if_no_output_annotation(self):
        def func(x: "foo"):
            pass

        with pytest.raises(QuarticException) as excinfo:
            step(func)

        assert "No output" in str(excinfo.value)


    def test_yields_inputs_including_flattened_multi_inputs(self):
        assert list(self.valid_step.inputs()) == [Dataset("foo"), Dataset("bar"), Dataset("bear")]


    def test_yields_outputs(self):
        assert list(self.valid_step.outputs()) == [Dataset("baz")]


    def test_yields_all_datasets(self):
        assert list(self.valid_step.datasets()) == [
            Dataset("foo"), Dataset("bar"), Dataset("bear"), Dataset("baz")
        ]


    def test_passes_args_to_executed_function(self):
        namespace = Mock()
        namespace.dataset.side_effect = lambda x: x
        quartic = Mock()
        quartic.return_value = namespace

        self.valid_step.execute(quartic, "nooblo")

        assert self.x == "foo"
        assert self.y == {"a": "bar", "b": "bear"}
        self.writer.apply.assert_called_with("baz")
