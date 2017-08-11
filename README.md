# Python bindings for Quartic platform
This repository contains Python bindings for the [Quartic platform](https://www.quartic.io).

## Getting set up

```
virtualenv .env --python=`which python3`
. .env/bin/activate
pip install -r requirements.txt
```

## Running tests

```
python -m pytest [-s]
```

## Adding new dependencies

Here are the rules:

- Things that are needed at runtime (i.e. when the bindings are deployed) should go in `setup.py`.
- Things that are only needed for dev/test (e.g. `pytest`) should go in `requirements.txt`.
- Always use pinned versions (i.e. `pyproj==1.9.5.1`, not `pyproj`) to maximise determinism.
- Never just dump the content of `pip freeze` into `setup.py`/`requirements.txt` - this includes transitive dependencies,
  making things very noisy and difficult to maintain.

## Notes on the repo layout

We're trending toward the layout described here:
*[Packaging a python library](https://blog.ionelmc.ro/2014/05/25/python-packaging/)*.

## License
This project is made available under the [BSD 3-Clause License](https://github.com/quartictech/quartic-python/blob/develop/LICENSE).


