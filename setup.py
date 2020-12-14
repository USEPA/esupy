from setuptools import setup

setup(
    name='esupy',
    version='0.0.1',
    packages=['esupy'],
    install_requires=['requests == 2.24.0',
                      'appdirs == 1.4.3',
                      'pandas == 1.1.3',
                      'requests_ftp == 0.3.1'],
    url='http://github.com/usepa/esupy',
    license='CC0',
    author='Wesley Ingwersen',
    author_email='ingwersen.wesley@epa.gov',
    description='Common objects for USEPA LCA ecosystem tools'
)
