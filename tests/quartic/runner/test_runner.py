import json
import os.path
import pytest

from quartic.runner.runner import main, parse_args, UserCodeExecutionException

class TestRunner:
    def test_evaluate_dag(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, "tests.quartic.runner.good_dag"])
        main(args)
        steps = json.load(open(output_path))
        assert len(steps) == 2

        steps.sort(key=lambda x: x["name"])
        assert steps[0]["name"] == "step1"
        assert steps[0]["description"] == "First step"
        assert steps[0]["file"] == "tests/quartic/runner/good_dag.py"
        assert steps[0]["line_range"] == [11, 14]

        assert steps[1]["name"] == "step2"
        assert steps[1]["description"] == "Second step"
        assert steps[1]["file"] == "tests/quartic/runner/good_dag.py"
        assert steps[1]["line_range"] == [16, 19]

    def test_evaluate_bad_dag(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, "tests.quartic.runner.bad_dag"])
        with pytest.raises(UserCodeExecutionException) as e:
            main(args)

    def test_execute_step(self, tmpdir):
        output_path = os.path.join(tmpdir, "steps.json")
        args = parse_args(["--evaluate", output_path, "tests.quartic.runner.good_dag"])
        main(args)

        steps = json.load(open(output_path))
        args = parse_args(["--execute", steps[0]["id"], "--namespace", "test", "tests.quartic.runner.good_dag"])
        main(args)

