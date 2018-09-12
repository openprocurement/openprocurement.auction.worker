from setuptools import setup, find_packages
import os

VERSION = '1.0.2a3+eauctions'

INSTALL_REQUIRES = [
    'setuptools',
    'openprocurement.auction',
    'WTForms',
    'WTForms-JSON',
]
EXTRAS_REQUIRE = {
    'test': [
        'pytest',
        'pytest-mock',
        'pytest-cov'
    ]
}
ENTRY_POINTS = {
    'console_scripts': [
        'auction_worker = openprocurement.auction.worker.cli:main',
    ],
    'openprocurement.auction.components': [
        'english = openprocurement.auction.worker.includeme:english_components'
    ],
    'openprocurement.auction.robottests': [
        'insider = openprocurement.auction.worker.tests.functional.main:includeme'
    ],
}

setup(name='openprocurement.auction.worker',
      version=VERSION,
      description="",
      long_description=open(os.path.join("docs", "HISTORY.txt")).read(),
      # Get more strings from
      # http://pypi.python.org/pypi?:action=list_classifiers
      classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
      ],
      keywords='',
      author='Quintagroup, Ltd.',
      author_email='info@quintagroup.com',
      license='Apache License 2.0',
      url='https://github.com/openprocurement/openprocurement.auction.worker',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['openprocurement', 'openprocurement.auction'],
      include_package_data=True,
      zip_safe=False,
      install_requires=INSTALL_REQUIRES,
      extras_require=EXTRAS_REQUIRE,
      entry_points=ENTRY_POINTS,
      )
