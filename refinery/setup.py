import os
from setuptools import setup, find_packages


setup(
    name='mdf_refinery',
    version='0.0.1',
    packages=find_packages(),
    description='Materials Data Facility python package',
    install_requires=[
        "mdf_forge>=0.0.1",
        "globus-sdk>=1.1.1",
        "requests>=2.18.1",
        "tqdm>=4.14.0",
        "ase>=3.14.1",
        "beautifulsoup4>=4.6.0",
        "xmltodict>=0.10.2",
        "pymatgen>=2017.6.8",
        "jsonschema>=2.6.0",
        "pymongo>=3.4.0"  # For bson.ObjectId
    ],
    package_data={'mdf_refinery': ['schemas/*.schema']}
)

from mdf_refinery import config
os.makedirs(config.MDF_PATH, exist_ok=True)
os.makedirs(config.PATH_DATASETS, exist_ok=True)
os.makedirs(config.PATH_FEEDSTOCK, exist_ok=True)
os.makedirs(config.PATH_CREDENTIALS, exist_ok=True)

