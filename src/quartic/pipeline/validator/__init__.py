# https://stackoverflow.com/questions/17583443/what-is-the-correct-way-to-share-package-version-with-setup-py-and-the-package # pylint: disable=line-too-long
from pkg_resources import get_distribution
__version__ = get_distribution('quartic-python').version
