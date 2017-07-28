from setuptools import setup

setup(
    name='mdf',
    version='0.0.1',
    packages=['mdf_dataworks'],
    description='Materials Data Facility python package',
    install_requires=[
        "globus-sdk>=1.1.1",
        "requests>=2.18.1",
        "tqdm>=4.14.0",
        "ase>=3.14.1",
        "beautifulsoup4>=4.6.0",
        "jsonschema>=2.6.0",
        "pymatgen>=2017.6.8",
        "pymongo>=3.4.0",
        "xmltodict>=0.10.2"
    ]
)