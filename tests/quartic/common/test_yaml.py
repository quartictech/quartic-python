from quartic.common import yaml_utils
import pytest
import os

class TestYamlConfigFinder:
    def test_no_config(self, tmpdir):
        assert yaml_utils.config_path(tmpdir) is None

    def test_config_in_dir(self, tmpdir):
        old_path = os.getcwd()
        os.chdir(tmpdir)
        yaml_utils.write_default()
        os.chdir(old_path)
        assert yaml_utils.config_path(tmpdir) is not None

    def test_config_in_parent_dir(self, tmpdir):
        old_path = os.getcwd()
        os.mkdir(os.path.join(tmpdir, 'test'))
        os.chdir(tmpdir)
        yaml_utils.write_default()
        os.chdir(old_path)
        assert yaml_utils.config_path(os.path.join(tmpdir, 'test')) is not None
