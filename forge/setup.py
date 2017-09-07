from setuptools import setup

setup(
    name='mdf_forge',
    version='0.4.0',
    packages=['mdf_forge'],
    description='Materials Data Facility python package',
    long_description="Forge is the Materials Data Facility Python package to interface and leverage the MDF Data Discovery service. Forge allows users to perform simple queries and facilitiates moving and synthesizing results.",
    python_requires='~=3.6',
    install_requires=[
        "globus-sdk>=1.1.1",
        "requests>=2.18.1",
        "tqdm>=4.14.0"
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Scientific/Engineering"
    ],
    keywords=[
        "MDF",
        "Materials Data Facility",
        "materials science",
        "dft",
        "data discovery",
        "supercomputing",
        "light sources"
    ],
    license="Apache License, Version 2.0",
    url="https://github.com/materials-data-facility/forge"
)
