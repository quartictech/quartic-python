import os
from setuptools import setup, find_packages

test_deps = [
          "mock==2.0.0",
          "pytest==3.0.7",
          "pylint==1.7.1",
          "pylint-quotes==0.1.5"
]

setup(name="quartic-python",
      version=os.environ.get("CIRCLE_BUILD_NUM", "0"),
      description="Quartic platform bindings",
      author="Quartic Technologies",
      author_email="contact@quartic.io",
      license="BSD",
      packages=find_packages("src"),
      include_package_data=True,
      package_dir={"":"src"},
      install_requires=[
          "click==6.7",
          "networkx==1.11",
          "gitpython==2.1.5",
          "pyaml==17.8.0"
      ],
      extras_require={
          "graphviz":["pygraphviz==1.3.1"],
          "ipython":["ipython==6.0.0"],
          "impl":[
              "pyarrow==0.4.0",
              "pyproj==1.9.5.1",
              "pandas==0.20.1",
              "requests==2.17.3",
              "datadiff==2.0.0"],
          "test":test_deps,
      },
      setup_requires=[
          "pytest-runner==2.11.1",
          "setuptools-lint==0.5.2"
      ],
      tests_require=test_deps,
      entry_points={
          "console_scripts": [
              "qli = quartic.pipeline.validator.cli:cli"
          ]
      })
