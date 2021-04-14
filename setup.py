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
        'cldfbench>=1.6.0',
        'clldutils>=3.7.0'
        'cldfcatalog>=1.3.0',
        'pycldf>=1.19.0'
        'pylexibank>=2.8.2',
        'pynumerals',
        'tqdm>=4.60.0',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
