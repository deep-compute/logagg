from setuptools import setup
from setuptools import find_packages

setup(
    name="logagg",
    version="0.2.7",
    description="logs aggregation framework",
    keywords="logagg",
    author="Deep Compute, LLC",
    author_email="contact@deepcompute.com",
    url="https://github.com/deep-compute/logagg",
    license='MIT',
    dependency_links=[
        "https://github.com/deep-compute/pygtail/tarball/master/#egg=pygtail-0.6.1",
    ],
    install_requires=[
        "basescript==0.1.13",
        "pymongo==3.6.0",
        "nsq-py==0.1.10",
        "influxdb==4.1.1",
        "deeputil==0.1.2",
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
        "License :: OSI Approved :: MIT License",
    ],
    test_suite='test.suite_maker',
    entry_points={
        "console_scripts": [
            "logagg = logagg:main",
        ]
    }
)
