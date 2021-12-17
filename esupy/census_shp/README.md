# Census Urban Area and Cluster (UAC) Shapefiles
Shapefiles (SHP) of Census urban areas and clusters can be obtained via [query](https://www.census.gov/cgi-bin/geo/shapefiles/index.php) or by navigating the [data server](https://www2.census.gov/geo/tiger/). Data review notes and exceptions to straight-forward implementation are as follows:

## UAC [2008](https://www2.census.gov/geo/tiger/TIGER2008/) and [2009](https://www2.census.gov/geo/tiger/TIGER2009/) SHPs
Per the [technical documentation](https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2009/TGRSHP09AA.pdf), the "\_uac00" and "\_uac" Urban Area shapefiles available for 2007-2009 differ. The latter relies on "corrected census 2000" data, which is likely preferable for statistical purposes. Therefore, we rely on the "_uac" corrected versions.
	
## UAC [2010](https://www2.census.gov/geo/tiger/TIGER2010/UA/2010/) and [2011](https://www2.census.gov/geo/tiger/TIGER2011/) SHPs
For 2010, a collection of six different SHP *tl_2010_xx_uac10.zip* files are available. However, the *tl_2010_us_uac10.zip* file contains all geometries present in the other  files except for *xx* = "78", which contains USVI geometries. Separately, 2011's geo/tiger/TIGER2011 directory contains no UAC files. Therefore we request and merge the two 2010 SHPs for both data years 2010 and 2011.

## Text Encoding
Pre-2015 SHPs [were encoded](https://www.census.gov/programs-surveys/geography/technical-documentation/user-note/special-characters.html) as ISO-8859-1 rather than UTF-8. 
Since UTF-8 is now the default encoding expected by `gpd.read_file()`, reading these pre-2015 SHPs generates a series of "WARNING Failed to decode ... using utf-8 codec" console messages.
While the [`gpd.read_file()` documentation](https://geopandas.org/en/stable/docs/reference/api/geopandas.read_file.html) indicates that an `encoding` kwarg (e.g., `encoding='iso-8859-1'`) can be passed to `fiona.open()`, attempting to do so currently returns a TypeError. As such, please ignore the "WARNING Failed to decode ... using utf-8 codec" messages, since they cannot be suppressed with the `warnings` library.
