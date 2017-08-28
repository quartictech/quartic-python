import os
from setuptools import setup, find_packages

setup(name="quartic-python",
      version=os.environ.get("CIRCLE_BUILD_NUM", "unknown"),
      description="Quartic platform bindings",
      author="Quartic Technologies",
      author_email="contact@quartic.io",
      license="BSD",
      packages=find_packages("src"),
      imclude_package_data=True,
      package_dir={"":"src"},
      install_requires=[
          "pyaml==17.8.0",
          "networkx==1.11",
          "click==6.7"
      ],
      extras_require={
          "graphviz":["pygraphviz==1.3.1"],
          "ipython":["ipython==6.0.0"],
          "impl":[
              "pyarrow==0.4.0",
              "pyproj==1.9.5.1",
              "pandas==0.20.1",
              "requests==2.17.3",
              "datadiff==2.0.0"]
      },
      entry_points={
          "console_scripts": [
              "qli = quartic.pipeline.validator.cli:cli"
          ]
      })
