# **esupy** : tool **e**co**s**ystem **u**tilities for Python

A Python library supporting Python-based tools in USEPA's tool ecosystem. The package itself provides no unique output.


## Installation Instructions for Optional Geospatial Packages

### conda, all platforms (recommended)
Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html) on your machine and create a new project directory containing env_sec_ctxt.yaml (obtained [from StEWI](https://github.com/USEPA/standardizedinventories/blob/e36902c3d1b23423381cd43c3bc8ac016a8219bb/env_sec_ctxt.yaml)).
Using the conda terminal to cd to this new directory, run `conda env create -f env_sec_ctxt.yaml`.
Once the optional geospatial depdencies and esupy (as a [StEWI dependency](https://github.com/USEPA/standardizedinventories/blob/master/setup.py)) are installed this way, urban/rural secondary context assignment should occur automatically when running StEWI code.

### pip, macOS & Ubuntu
The versions of Fiona and GDAL necessary to support GeoPandas are available for macOS and Ubuntu machines up through Python 3.9, but wheels for 3.10 are still [forthcoming](https://github.com/geopandas/geopandas/issues/2212)).
Shapely also [has yet to publish](https://github.com/shapely/shapely/issues/1215) Python 3.10 wheels, and so Python versions 3.7-3.9 are recommended.


## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer
has responsibility to protect the integrity , confidentiality, or availability of the information.  Any
reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal
and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or
the United States Government.

 