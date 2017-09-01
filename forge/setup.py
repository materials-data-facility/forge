from setuptools import setup

setup(
    name='mdf_forge',
    version='0.4.0',
    packages=['mdf_forge'],
    description='Materials Data Facility python package',
    install_requires=[
        "globus-sdk>=1.1.1",
        "requests>=2.18.1",
        "tqdm>=4.14.0"
    ]
)
