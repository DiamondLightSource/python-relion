=========================
Python bindings to RELION
=========================


.. image:: https://img.shields.io/pypi/v/relion.svg
        :target: https://pypi.python.org/pypi/relion
        :alt: PyPI release

.. image:: https://img.shields.io/pypi/pyversions/relion.svg
        :target: https://pypi.python.org/pypi/relion
        :alt: Supported Python versions

.. image:: https://img.shields.io/lgtm/alerts/g/DiamondLightSource/python-relion.svg?logo=lgtm&logoWidth=18
        :target: https://lgtm.com/projects/g/DiamondLightSource/python-relion/alerts/
        :alt: Total alerts

.. image:: https://readthedocs.org/projects/python-relion/badge/?version=latest
        :target: https://python-relion.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://codecov.io/gh/DiamondLightSource/python-relion/branch/main/graph/badge.svg
        :target: https://codecov.io/gh/DiamondLightSource/python-relion
        :alt: Test coverage

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
        :target: https://github.com/ambv/black
        :alt: Code style: black


* Free software: GPLv2 and BSD, `see the license file for details <https://github.com/DiamondLightSource/python-relion/blob/main/LICENSE>`_
* Documentation: https://python-relion.readthedocs.io.

This package provides a python interface to the information contained in a Relion project folder. It does not run Relion itself.

Currently it caters for specific fields from the Motion Correction, CTF Find, 2D Classification and 3D Classification stages of the Relion pipeline, but this could readily be expanded to more stages and fields.
