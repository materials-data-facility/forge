.. highlight:: rst

MDF Forge
=========

Forge is the Materials Data Facility Python package to interface and
leverage the MDF Data Discovery service. Forge allows users to perform
simple queries and facilitiates moving and synthesizing results.

..  toctree::
    :titlesonly:

    forge_quickstart

Installation
============

.. code-block:: bash

   pip install mdf_forge

..  toctree::
    :titlesonly:

    installation_guide.rst

For Developers
--------------

.. image:: https://img.shields.io/pypi/v/mdf_forge.svg
   :target: https://pypi.python.org/pypi/mdf-forge
.. image:: https://travis-ci.org/materials-data-facility/forge.svg?branch=master
   :target: https://travis-ci.org/materials-data-facility/forge
.. image:: https://coveralls.io/repos/github/materials-data-facility/forge/badge.svg?branch=master
   :target: https://coveralls.io/github/materials-data-facility/forge?branch=master

.. code-block:: bash

   git clone https://github.com/materials-data-facility/forge.git
   cd forge
   pip install -e .

..  toctree::
    :maxdepth: 2

    requirements_link
    mdf_forge

Documentation and examples
==========================

Documentation, including tutorials and examples, can be found in the
``docs`` directory.

Tutorials
---------

#. Introduction (`Jupyter Notebook <https://github.com/materials-data-facility/forge/blob/master/docs/tutorials/1%20-%20Introduction.ipynb/>`_)
#. Core Query Builder Functions (`Jupyter Notebook <https://github.com/materials-data-facility/forge/blob/master/docs/tutorials/2%20-%20Core%20Query%20Builder%20Functions.ipynb/>`_)
#. Expanded Query Builder Functions (`Jupyter Notebook <https://github.com/materials-data-facility/forge/blob/master/docs/tutorials/3%20-%20Expanded%20Query%20Builder%20Functions.ipynb/>`_)
#. General Helper Functions (`Jupyter Notebook <https://github.com/materials-data-facility/forge/blob/master/docs/tutorials/4%20-%20General%20Helper%20Functions.ipynb/>`_)
#. Field-Specific Helper Functions (`Jupyter Notebook <https://github.com/materials-data-facility/forge/blob/master/docs/tutorials/5%20-%20Field-Specific%20Helper%20Functions.ipynb/>`_)
#. Data Retrieval Functions (`Jupyter Notebook <https://github.com/materials-data-facility/forge/blob/master/docs/tutorials/6%20-%20Data%20Retrieval%20Functions.ipynb/>`_)

Examples
--------

#. Example Aggregations (`Jupyter Notebook <https://github.com/materials-data-facility/forge/blob/master/docs/examples/Example%20Aggregations.ipynb/>`_)
#. Example Statistics - MDF Datasets (`Jupyter Notebook <https://github.com/materials-data-facility/forge/blob/master/docs/examples/Example%20Statistics%20-%20MDF%20Datasets.ipynb/>`_)

Requirements
============

-  Forge requires Python 2.7 or >=3.3
-  To access data in the MDF, you must have an account recognized by
   Globus Auth (including Google, ORCiD, many academic institutions, or
   a `free Globus ID`_).

Contributions
=============

If you find a bug or want a feature, feel free to open an issue here on
GitHub (and please tag it accordingly). If you want to contribute code
yourself, we’re more than happy to accept merge requests.

Support
=======

This work was performed under financial assistance award 70NANB14H012
from U.S. Department of Commerce, National Institute of Standards and
Technology as part of the `Center for Hierarchical Material Design
(CHiMaD)`_. This work was also supported by the National Science
Foundation as part of the `Midwest Big Data Hub`_ under NSF Award
Number: 1636950 “BD Spokes: SPOKE: MIDWEST: Collaborative: Integrative
Materials Design (IMaD): Leverage, Innovate, and Disseminate”.

.. _free Globus ID: https://www.globusid.org/create
.. _Center for Hierarchical Material Design (CHiMaD): http://chimad.northwestern.edu
.. _Midwest Big Data Hub: http://midwestbigdatahub.org

----

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


