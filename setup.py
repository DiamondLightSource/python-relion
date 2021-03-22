#!/usr/bin/env python

import setuptools

console_scripts = ["relipy.show=relion.cli.pipeline_viewer:run"]
setuptools.setup(
    entry_points={"console_scripts": console_scripts},
)
