#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from setuptools import setup, find_packages

python_version = sys.version_info

if python_version < (3, 4, 0): raise Exception('This package requires >= 3.4.0')

version = '0.1'
name = 'porn_chatbot_py'
short_description = ''
author = 'Kensuke Mitsuzawa'
author_email = ''
url = ''
license = 'MIT'
description = ''

install_requires = [
    'beautifulsoup4',
    'requests'
]

dependency_links = [
]

setup(
    name=name,
    version=version,
    description=short_description,
    author=author,
    install_requires=install_requires,
    dependency_links=dependency_links,
    author_email=author_email,
    url=url,
    license=license,
    packages=find_packages(),
    test_suite='tests',
    include_package_data=True,
    zip_safe=False
)