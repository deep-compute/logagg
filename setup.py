from setuptools import setup
from setuptools import find_packages

setup(
    name="logagg",
    version="0.1",
    description="Collect all the logs from server and parses"
                "it to get common schema for all the logs and"
                "stores at common location `MongoDB`.",
    keywords="logagg",
    author="Deep Compute, LLC",
    author_email="contact@deepcompute.com",
    url="https://github.com/deep-compute/logagg",
    license='GPL',
    dependency_links=[
        "https://github.com/deep-compute/pygtail/tarball/master/#egg=pygtail-0.6.1",
    ],
    install_requires=[
        "basescript",
        "pymongo",
        "nsq-py",
        "pygtail==0.6.1",
        "influxdb",
    ],
    package_dir={'logagg': 'logagg'},
    packages=find_packages('.'),
    include_package_data=True,
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: GNU General Public License (GPL)"
    ],
    entry_points={
        "console_scripts": [
            "logagg = logagg:main",
        ]
    }
)
