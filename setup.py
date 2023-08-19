from setuptools import setup, find_packages

setup(
    name="package_name",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)
