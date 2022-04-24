import setuptools

with open("README.md", "r", encoding="utf-8") as readme:
    long_description = readme.read()

setuptools.setup(
    name="config-parser-sobasudo",
    version="0.0.1",
    author="Ranmal Dias",
    author_email="dev.mendy.das@gmail.com",
    description="quick parser for config files for sqlalchemy",
    long_description=long_description,
    url="https://github.com/sobasudo",
    package_dir={"":"db", "utils":"utils"},
    packages=setuptools.find_packages(where="db"),
    python_requires=">=3.6"
)