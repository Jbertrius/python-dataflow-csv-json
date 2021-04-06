import setuptools


REQUIRED_PACKAGES = [
    'apache-beam[gcp]',
    'pylint'
]

PACKAGE_NAME = 'TheCodebuzzDataflow'
PACKAGE_VERSION = '0.0.1'

setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Set up file for the required Dataflow packages ',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)