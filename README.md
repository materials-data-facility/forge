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
from mdf_dataworks.forge import Forge

mdf = forge.Forge()

# free text query
res = mdf.search("materials commons")

# structured query
res_s = mdf.search_by_elements(elements=["Al","Cu"], sources=["oqmd"])
```

More examples are available at [Forge Docs](https://github.com/materials-data-facility/forge-docs)


# Documentation
Documentation is available at [Forge Docs](https://github.com/materials-data-facility/forge-docs)


# Support
This work was performed under financial assistance award 70NANB14H012 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the [Center for Hierarchical Material Design (CHiMaD)](http://chimad.northwestern.edu).
