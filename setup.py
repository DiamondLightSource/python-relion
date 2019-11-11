#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from setuptools import setup, find_packages

requirements = ["gemmi==0.2.8", "numpy==1.14.5"]
setup_requirements = []

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="relion_yolo_it",
    author="Donovan Webb",
    author_email="donovan.webb@diamond.ac.uk",
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Natural Language :: English",
    ],
    description="Updated relion_it with cryolo wrappers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=requirements,
    license="GPLv2",
    include_package_data=True,
    packages=find_packages(),
    scripts=[
        "relion_yolo_it/cryolo_relion_it.py",
        "relion_yolo_it/CryoloPipeline.py",
        "relion_yolo_it/CryoloExternalJob.py",
        "relion_yolo_it/CryoloFineTuneJob.py",
    ],
    python_requires="==3.6.8",
    setup_requires=setup_requirements,
    version="0.2.5",
    zip_safe=False,
)
