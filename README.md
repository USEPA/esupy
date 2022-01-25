# **esupy** : tool **e**co**s**ystem **u**tilities for Python

A Python library supporting Python-based tools in USEPA's tool ecosystem. The package itself provides no unique output.


## Installation Instructions for Optional Geospatial Packages

### conda (recommended)
Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html) on your machine and create a new project directory containing env_geo.yaml. Using the conda terminal to cd to this new directory, run `conda env create -f env_geo.yaml`.
Once esupy and its optional geospatial depdencies are installed this way, other EPA packages (such as [StEWI](https://github.com/USEPA/standardizedinventories)) can be installed via pip VCS call and will recognize their esupy dependency as satisfied.

### pip, macOS & Ubuntu
The versions of Fiona and GDAL necessary to support GeoPandas are available for macOS and Ubuntu machines up through Python 3.9, but wheels for 3.10 are still [forthcoming](https://github.com/geopandas/geopandas/issues/2212)).
Shapely also [has yet to publish ](https://github.com/shapely/shapely/issues/1215) Python 3.10 wheels, and so Python versions 3.7-3.9 are recommended.

### pip, windows
Installation on Windows is more involved. The simplest (albeit unofficial) route uses [pipwin](https://pypi.org/project/pipwin/) to retrieve and install [Christoph Gohlke's windows binaries](https://www.lfd.uci.edu/~gohlke/pythonlibs/) of GDAL, Fiona, and GeoPandas. 
Call `pip install pipwin` followed by `pipwin install -r pipwin_requirements.txt` to install the binaries (plus `pipwin install shapely` if attempting a 3.10 build). 
Upon completing the pipwin installs, the remainder of the packages can be normally pip installed via geo_requirements.txt.


## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer
has responsibility to protect the integrity , confidentiality, or availability of the information.  Any
reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal
and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or
the United States Government.

 