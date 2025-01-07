import io

from setuptools import find_packages, setup

version = "4.1.0"

with io.open("./README.md", encoding="utf-8") as f:
    readme = f.read()

setup(
    name="pyrdp",
    version=version,
    license="MIT",
    url="",
    author="Robert Li",
    author_email="laigp@hotmail.com",
    description="Pure Python RapidsDB Driver",
    long_description=readme,
    packages=find_packages(exclude=["tests*", "pyrdp.tests*"]),
    # packages=find_packages(),
    entry_points={
        "sqlalchemy.dialects": [
            "rapidsdb.pyrdp = pyrdp.sa.pyrdp:RDPDialect_pyrdp",
        ]
    },
    install_requires=["thrift==0.11.0"],
    zip_safe=False,
    classifiers=[
        "Development Status :: 1",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Intended Audience :: Developers",
        "Topic :: Database",
    ],
)
