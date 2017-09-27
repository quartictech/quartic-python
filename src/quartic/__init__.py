from pkg_resources import get_distribution

from quartic.dsl.step import step
from quartic.common.dataset import writer

__version__ = get_distribution("quartic-python").version
