from setuptools import setup

setup(
    name='esupy',
    version='0.2.0',
    packages=['esupy'],
    include_package_data=True,
    python_requires=">=3.7",
    install_requires=['requests>=2.22.0',
                      'appdirs>=1.4.3',
                      'pandas>=1.1.0',
                      'requests_ftp>=0.3.1',
                      'pyarrow>=4.0.0',
                      'pyyaml>=5.3',
                      'numpy>=1.20.1',
                      'boto3>=1.23.0',
                      ],
    extras_require={"urban_rural": ['geopandas>=0.9.0; platform_system!="Windows"',
                                    'shapely>=1.7.1; platform_system!="Windows"']},
    url='http://github.com/usepa/esupy',
    license='CC0',
    author='Wesley Ingwersen',
    author_email='ingwersen.wesley@epa.gov',
    description='Common objects for USEPA LCA ecosystem tools'
)
