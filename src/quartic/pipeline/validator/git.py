import uuid
import os
import datetime
from git import Repo, RemoteProgress
from quartic.common import yaml_utils, utils
import click

class ProgressPrinter(RemoteProgress):
    def __init__(self):
        RemoteProgress.__init__(self)
        self.pbar = click.progressbar(length=3)
    def update(self, op_code, cur_count, max_count=None, message=""):
        self.pbar.update(1)

def check_git_dir():
    """Expect config to be in the same place as root of git repo."""
    cpath = yaml_utils.config_path()
    cdir = os.path.dirname(cpath)
    return os.path.isdir(os.path.join(cdir, ".git"))

def get_git_dir():
    cdir = os.path.dirname(yaml_utils.config_path())
    if check_git_dir():
        return os.path.join(cdir, ".git")
    else:
        return None

def get_git_repo():
    repo = Repo(get_git_dir())
    assert not repo.bare
    return repo

def create_test_branch(repo):
    return repo.create_head("quartic/{}".format(uuid.uuid4()))

def commit_push(repo, branch):
    cfg = yaml_utils.config()
    old_branch = repo.active_branch
    br = branch.checkout()
    index = repo.index
    try:
        pdir = yaml_utils.attr_paths_from_config(cfg["pipeline_directory"])
        index.add(utils.get_python_files(pdir))
        index.commit("Quartic - qli commit on {}".format(datetime.date.today()))
        origin = repo.remotes.origin
        origin.push("{}:{}".format(branch.name, branch.name), progress=ProgressPrinter())
        repo.head.reset("HEAD~")
    #pylint: disable=W0703
    except Exception as e:
        print(e)
    old_branch.checkout()
    repo.delete_head(br, force=True)
