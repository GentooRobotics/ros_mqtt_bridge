#!/usr/bin/env python3

from setuptools import setup, find_packages
from catkin_pkg.python_setup import generate_distutils_setup

# fetch values from package.xml
setup_args = generate_distutils_setup(
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)

setup(**setup_args)
