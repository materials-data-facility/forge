# Forge
Forge is the Materials Data Facility python package to interface and leverage the MDF Data Discovery service. 

# Installation
```
git clone https://github.com/materials-data-facility/forge.git
cd forge
pip install -e .
```

# Examples

```python
from mdf_forge.forge import Forge

mdf = Forge()

# free text query
res = mdf.search("materials commons")

# structured query
res_s = mdf.search_by_elements(elements=["Al","Cu"], sources=["oqmd"])
```

More examples are available in the examples directory.


# Documentation
Documentation is available in the docs directory.


# Support
This work was performed under financial assistance award 70NANB14H012 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the [Center for Hierarchical Material Design (CHiMaD)](http://chimad.northwestern.edu). This work was also supported by the National Science Foundation as part of the [Midwest Big Data Hub](http://midwestbigdatahub.org) under NSF Award Number: 1636950 "BD Spokes: SPOKE: MIDWEST: Collaborative: Integrative Materials Design (IMaD): Leverage, Innovate, and Disseminate".

