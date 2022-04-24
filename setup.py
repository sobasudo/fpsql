import setuptools

with open("README.md", "r", encoding="utf-8") as readme:
    long_description = readme.read()

setuptools.setup(
    name="dal_sqlalchemy_sobasudo",
    version="0.0.1",
    author="Ranmal Dias",
    author_email="dev.mendy.das@gmail.com",
    description="quick config for new and existing databases",
    long_description=long_description,
    url="https://github.com/sobasudo",
    package_dir={"":"src", "db":"src/db", "utils":"src/utils"},
    packages=setuptools.find_packages(where="src"),
    install_requires= ["alembic>=1.7.7", "autopep8>=1.6.0", "cx-Oracle>=8.3.0", "greenlet>=1.1.2", "Mako>=1.2.0", "MarkupSafe>=2.1.1", "psycopg2>=2.9.3", "pycodestyle>=2.8.0", "pymssql>=2.2.5", "PyMySQL>=1.0.2", "SQLAlchemy>=1.4.35", "toml>=0.10.2"],
    python_requires=">=3.6"
)