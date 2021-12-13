from setuptools import setup

setup(
    name='esupy',
    version='0.1.7',
    packages=['esupy'],
    install_requires=['requests>=2.22.0',
                      'appdirs>=1.4.3',
                      'numpy>=1.20.1',
                      'pandas>=1.1.0',
                      'geopandas>=0.9.0',
                      'shapely>=1.7.1',
                      'requests_ftp>=0.3.1',
                      'pyarrow>=4.0.0',
                      ],
    url='http://github.com/usepa/esupy',
    license='CC0',
    author='Wesley Ingwersen',
    author_email='ingwersen.wesley@epa.gov',
    description='Common objects for USEPA LCA ecosystem tools'
)
