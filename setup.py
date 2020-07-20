from setuptools import setup
import json


with open("metadata.json") as fp:
    metadata = json.load(fp)


setup(
    name='lexibank_numerals',
    description=metadata['title'],
    license=metadata.get('license', ''),
    url=metadata.get('url', ''),
    py_modules=['lexibank_numerals'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'lexibank.dataset': [
            'numerals=lexibank_numerals:Dataset',
        ]
    },
    install_requires=[
        'cldfcatalog',
        'pylexibank>=2.1',
        'pynumerals',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
