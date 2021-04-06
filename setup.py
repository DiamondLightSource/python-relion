#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from setuptools import setup, find_packages

requirements = ["gemmi>=0.2.8", "numpy>=1.14.5"]
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
    description="Updated relion_it with cryolo wrappers for Diamond Light Source. For Relion 3.1. Thanks to Yuriy Chaban, Josh Lobo and Sean Connell.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=requirements,
    license="GPLv2",
    include_package_data=True,
    packages=find_packages(),
    scripts=[
        "relion_yolo_it/cryolo_relion_it.py",
        "relion_yolo_it/cryolo_external_job.py",
        "relion_yolo_it/cryolo_fine_tune_job.py",
        "relion_yolo_it/select_and_split_external_job.py",
        "relion_yolo_it/mask_soft_edge_external_job.py",
        "relion_yolo_it/reconstruct_halves_external_job.py",
        "relion_yolo_it/fsc_fitting_external_job.py",
    ],
    entry_points={
        "zocalo.wrappers": [
            "relion = relion_yolo_it.relion_zocalo_wrapper:RelionWrapper"
        ]
    },
    python_requires=">=3.6",
    setup_requires=setup_requirements,
    url="https://github.com/DiamondLightSource/python-relion-yolo-it",
    version="0.4.0",
    zip_safe=False,
)
