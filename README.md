# Forge
[![PyPI](https://img.shields.io/pypi/v/mdf_forge.svg)](https://pypi.python.org/pypi/mdf-forge) [![Build Status](https://travis-ci.org/materials-data-facility/forge.svg?branch=master)](https://travis-ci.org/materials-data-facility/forge) [![Coverage Status](https://coveralls.io/repos/github/materials-data-facility/forge/badge.svg?branch=master)](https://coveralls.io/github/materials-data-facility/forge?branch=master)

Forge is the Materials Data Facility Python package to interface and leverage the MDF Data Discovery service. Forge allows users to perform simple queries and facilitiates moving and synthesizing results.

# Installation

```
pip install mdf_forge
```

### For Developers
```
git clone https://github.com/materials-data-facility/forge.git
cd forge
pip install -e .
```

### Jupyter Environment
We use Docker to create a pre-built container for the
[scipy Jupyter environment](https://hub.docker.com/r/jupyter/scipy-notebook/)
along with Forge and required dependencies.

To build a docker image
```
git clone https://github.com/materials-data-facility/forge.git
cd forge
docker build -t forge-notebook .
docker run -it --rm -p 8888:8888 forge-notebook
```

Note that by default the docker image runs with no authentication. This is not
recommended if the service is accessible from the internet. You can secure
your service by generating a key:

```
% openssl rand -base64 32
```

And passing that key as an environment variable to the docker container:

```
% docker run -it --rm -e APP_TOKEN=yourtoken -p 8888:8888 forge-notebook
```

You can then visit http://localhost:8888 to launch a forge notebook. All of the
examples from this repo can be found in the `work/examples` folder in Jupyter.

# Examples

```python
from mdf_forge.forge import Forge

mdf = Forge()

# free text query
r = mdf.search("materials commons")

# structured query
r_2 = mdf.search_by_elements(elements=["Al","Cu"], sources=["oqmd"])
```

More examples are available in the docs/examples directory.


# Documentation
Documentation is available in the docs directory.

# Requirements
* Forge requires Python 2.7 or >=3.3 
* To access data in the MDF, you must have an account recognized by Globus Auth (including Google, ORCiD, many academic institutions, or a [free Globus ID](https://www.globusid.org/create)).

# Contributions
If you find a bug or want a feature, feel free to open an issue here on GitHub (and please tag it accordingly). If you want to contribute code yourself, we're more than happy to accept merge requests.



# Support
This work was performed under financial assistance award 70NANB14H012 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the [Center for Hierarchical Material Design (CHiMaD)](http://chimad.northwestern.edu). This work was also supported by the National Science Foundation as part of the [Midwest Big Data Hub](http://midwestbigdatahub.org) under NSF Award Number: 1636950 "BD Spokes: SPOKE: MIDWEST: Collaborative: Integrative Materials Design (IMaD): Leverage, Innovate, and Disseminate".

