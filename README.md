# PARSER for SQLAlchemy

A fast and easy configuration for sqlalchemy databases. Get all tables up and running and have it all in the same place.

## Sample Configurations:
For cfg or ini configs:
```ini
[database]
dbapi = postgresql
username = dev
password = dev123
server = localhost
port = 5432
database = sample
```

For Sqlite:
```ini
[database]
dbapi = sqlite
source = sample
echo = True
```

For JSON configs:
```json
{
    "dbapi": "mysql+pymysql",
    "username": "dev",
    "password": "dev123",
    "server": "localhost",
    "port": "5432",
    "database": "sample"
}
```

## Basic Usage:

```python
from db.dal import Database

db = Database(config=r"path/to/config_file.ini")

db.create_table('user', ondelete='CASCADE')\
                .col('uid', 'Integer', primary_key=True)\
                .col('name', 'String', nullable=False)
db.try_create()
```

The `Database` object can be configured by using `config` or `json_config`.

### Customization
To use the Sqlalchemy provided Table constructs, simply use the exposed `Database.engine` and `Database.metadata`.

These are preconfigured to use the settings provided in the config file.

To bypass `Database` entirely, simply import `json_engine` or `config_engine` from `db.parser`.

`source` in the ini or json config file is entirely optional. It accepts any filename other than `memory`, in which case it creates an Sqlite in-memory database.