# **esupy** : tool **e**co**s**ystem **u**tilities for Python

A Python library supporting Python-based tools in USEPA's tool ecosystem. The package itself provides no unique output.


## Installation Instructions for Optional Geospatial Packages

### conda, all platforms (recommended)
Install [Anaconda](https://www.anaconda.com/products/distribution) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html) on your machine. Then create a new project directory and insert a copy of env_sec_ctxt.yaml (obtained [from StEWI](https://github.com/USEPA/standardizedinventories/blob/master/env_sec_ctxt.yaml)).
Using the conda terminal, cd to this new directory and run `conda env create -f env_sec_ctxt.yaml`.

Once the optional geospatial depdencies and esupy (obtained as a [StEWI dependency](https://github.com/USEPA/standardizedinventories/blob/master/setup.py)) are installed this way, urban/rural and release height secondary context assignment should occur automatically when generating inventory files in StEWI.

### pip, macOS & Ubuntu
The versions of Fiona and GDAL necessary to support GeoPandas are available for macOS and Ubuntu machines up through Python 3.9, but their wheels for 3.10 are still [forthcoming](https://github.com/geopandas/geopandas/issues/2212).
Shapely also [has yet to publish](https://github.com/shapely/shapely/issues/1215) Python 3.10 wheels, so only Python versions 3.7-3.9 are supported.


## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer
has responsibility to protect the integrity , confidentiality, or availability of the information.  Any
reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal
and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or
the United States Government.

 