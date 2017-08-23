import json
import os.path
import pytest
from mock import Mock, MagicMock
from quartic import QuarticException
from quartic.common.step import step, Step
from quartic.common.dataset import Dataset, writer
from quartic.pipeline.runner.cli import main, parse_args, UserCodeExecutionException

class TestCli:
    def test_evaluate_dag(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, "tests/quartic/pipeline/good_dag.py"])
        main(args)
        steps = json.load(open(output_path))
        assert len(steps) == 2

        steps.sort(key=lambda x: x["name"])
        assert steps[0]["name"] == "step1"
        assert steps[0]["description"] == "First step"
        assert steps[0]["file"] == "tests/quartic/pipeline/good_dag.py"
        assert steps[0]["line_range"] == [11, 14]

        assert steps[1]["name"] == "step2"
        assert steps[1]["description"] == "Second step"
        assert steps[1]["file"] == "tests/quartic/pipeline/good_dag.py"
        assert steps[1]["line_range"] == [16, 19]

    def test_evaluate_bad_dag(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, "tests/quartic/pipeline/bad_dag.py"])
        with pytest.raises(UserCodeExecutionException) as e:
            main(args)

    def test_execute_step(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, "tests/quartic/pipeline/good_dag.py"])
        main(args)

        steps = json.load(open(output_path))
        args = parse_args(["--execute", steps[0]["id"], "--namespace", "test", "tests/quartic/pipeline/good_dag.py"])
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

        assert dataset.writer.called_with("foo", "bar")

    def test_parquet(self):
        dataset = MagicMock()
        writer("foo", "bar").parquet(42).apply(dataset)

        assert dataset.writer.__enter__.parquet.called_with(42)


    def test_json(self):
        dataset = MagicMock()
        writer("foo", "bar").json(42).apply(dataset)

        assert dataset.writer.__enter__.json.called_with(42)



class TestStep:
    def setup_method(self, method):
        # pylint: disable=attribute-defined-outside-init
        self.writer = Mock()
        def func(x: "foo", y: {"a": "bar", "b": "bear"}) -> "baz":
            self.x = x
            self.y = y
            return self.writer
        self.valid_step = step(func)


    def test_decorator_applies_step_wrapper(self):
        @step
        def func() -> "alice/bob":
            pass

        assert isinstance(func, Step)


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
