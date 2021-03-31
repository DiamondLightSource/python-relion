#!/usr/bin/env python

import setuptools

console_scripts = ["relipy.show=relion.cli.pipeline_viewer:run"]

if __name__ == "__main__":
    setuptools.setup(
        entry_points={"console_scripts": console_scripts},
    )
