import io

from setuptools import find_packages, setup  # type: ignore

version = "1.0"

with io.open("./README.md", encoding="utf-8") as f:
    readme = f.read()

setup(
    name="pydbgen",
    version=version,
    license="MIT",
    url="",
    author="Robert Li",
    author_email="laigp@hotmail.com",
    description="TPC-H Python data tool",
    long_description=readme,
    packages=find_packages(exclude=["tests*", "pydbgen.tests*"]),
    # packages=find_packages(),
    entry_points={},
    install_requires=[],
    zip_safe=False,
    classifiers=[
        "Development Status :: 1",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Intended Audience :: Developers",
        "Topic :: Database",
    ],
)
