from setuptools import setup

setup(
    name='esupy',
    version='0.1.1',
    packages=['esupy'],
    install_requires=['requests >=2.22.0',
                      'appdirs>=1.4.3',
                      'pandas>=1.1.0',
                      'requests_ftp>=0.3.1',
                      'selenium>=3.141.0'],
    url='http://github.com/usepa/esupy',
    license='CC0',
    author='Wesley Ingwersen',
    author_email='ingwersen.wesley@epa.gov',
    description='Common objects for USEPA LCA ecosystem tools'
)
