import os
from setuptools import setup, find_packages

setup(name="quartic-python",
      version=os.environ.get("CIRCLE_BUILD_NUM", "unknown"),
      description="Quartic platform bindings",
      author="Quartic Technologies",
      version="0.1",
      author_email="contact@quartic.io",
      license="BSD",
      packages=find_packages("src"),
      package_dir={"":"src"},
      install_requires=[
          "requests==2.17.3",
          "ipython==6.0.0",
          "datadiff==2.0.0",
          "pyarrow==0.4.0",
          "pyproj==1.9.5.1",
          "pandas==0.20.1",
          "networkx==1.11"
      ],
      zip_safe=False,
      scripts=['bin/qli']
      )
