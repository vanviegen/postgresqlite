# PostgreSQLite

Python module that gives you the power of a PostgreSQL server, with the convenience of the `sqlite3` module.

## Features

- Using just a `postgresqlite.connect()` call, the library will automatically...
    - Download and install PostgreSQL into the user's `~/.cache` directory. (Linux and macOS EM64T only, for now.)
    - Create a new database (`initdb`) within the project directory with a random password.
    - Start the PostgreSQL server.
    - Set up a DB-API connection to the server (using the `pg8000` driver).
    - Shut down the server when it's no longer in use.
- It also adds a couple of conveniences on top of DB-API, making it more similar to the `sqlite3` module:
    - Calling `execute` on the connection will create a new cursor.
    - Calls to `fetchall` and `fetchone` return objects that can address fields both by number (as is standard for DB-API) as well as by name (as `sqlite3` offers when you configure `connection.row_factory = sqlite3.Row`).
    - Autocommit mode is enabled by default.
    - Parameterized queries use `?` as a placeholder. (`paramstyle = 'qmark'`)
- It can open `psql` and other PostgreSQL clients passing in connection details, while making sure the database is running.
- For use in production, the configuration file can be modified to point your application at a (non-auto-starting) PostgreSQL server.


## Examples

### Using DB-API directly

```sh
pip install postgresqlite
```

```python
import postgresqlite

db = postgresqlite.connect(mode='dbapi')

cursor = db.cursor()
cursor.execute('create table if not exists tests(id serial, info text not null, created_at timestamp not null default current_timestamp)')
cursor.execute("insert into tests(info) values('Hi mom!'),('This is great!')")
db.commit()

cursor.execute("select id, info, created_at from tests where id=%s", [1])
for row in cursor:
    print("row:", row, "id:", row[0], "created_at:", row[2])

cursor.execute("select count(*) from tests")
print("count:", cursor.fetchone()[0])

cursor.execute("select * from tests order by id desc limit 1")
print("row:", cursor.fetchone())
```

### Using the `FriendlyConnection` API

```sh
pip install postgresqlite
```

```python
import postgresqlite

db = postgresqlite.connect()

db.query('create table if not exists tests(id serial, info text not null, created_at timestamp not null default current_timestamp)')
db.query("insert into tests(info) values('Hi mom!'),('This is great!')")

rows = db.query("select * from tests where id=:id", id=1)
for row in rows:
    print("row:", row, "id:", row.id, "created_at:", row.created_at)

print("count:", db.query_value("select count(*) from tests"))

print("last row:", db.query_row("select * from tests order by id desc limit 1"))
```

### Using Flask-SQLAlchemy

```sh
pip install postgresqlite flask_sqlalchemy
```

```python
import postgresqlite, flask, flask_sqlalchemy, datetime

app = flask.Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = postgresqlite.get_uri()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = flask_sqlalchemy.SQLAlchemy(app)

class Car(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    brand = db.Column(db.String)
    model = db.Column(db.String, nullable=False)
    day_rate = db.Column(db.Integer)
    rentals = db.relationship('Rental', backref='car')

    def to_dict(self):
        return {'id': self.id, 'brand': self.brand, 'model': self.model, 'day_rate': self.day_rate, 'rentals': [rental.to_dict() for rental in self.rentals]}
    
class Rental(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    car_id = db.Column(db.Integer, db.ForeignKey('car.id'), nullable=False)

    customer_name = db.Column(db.String, nullable=False)
    start_time = db.Column(db.DateTime, default=datetime.datetime.now)
    end_time = db.Column(db.DateTime)

    def to_dict(self):
        return {'id': self.id, 'car_id': self.car_id, 'customer_name': self.customer_name, 'start_time': self.start_time, 'end_time': self.end_time}

@app.route('/cars')
def show_all():
   return flask.jsonify([car.to_dict() for car in Car.query.all()])

@app.route('/cars', methods = ['POST'])
def new_car():
    car = Car(**flask.request.get_json())
    db.session.add(car)
    db.session.commit()
    return flask.jsonify(car.to_dict())

@app.route('/rentals', methods = ['POST'])
def new_rental():
    rental = Rental(**flask.request.get_json())
    db.session.add(rental)
    db.session.commit()
    return flask.jsonify(rental.to_dict())

# Create tables based on the Model classes above.
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(debug = True)
```

Running this should expose a REST API. If you have [httpie](https://httpie.io/) installed, it can be tested from the command like this:

```sh
http http://127.0.0.1:5000/cars brand=Volvo model=S60
http http://127.0.0.1:5000/rentals car_id=1 customer_name=Frank
http http://127.0.0.1:5000/cars
```



## Documentation

### API functions

#### `postgresqlite.connect(dirname='data/postgresqlite', mode='friendly', config=None)`

Start a server (if needed), wait for it, and return a DB-API compatible object.

Arguments:
- `dirname` (str): The dir where the configuration file (`postgresqlite.json`)
    will be read or created, and where database files will be stored. If the
    path does not exist, it will be created.
- `mode` ('easy', 'sqlite', 'dbapi'): 
  - When set to `friendly` (the default), a `FriendlyConnection` object is returned. It's a DB-API compatible Connection, but with a few additions to make it more programmer-friendly, as documented in the [The FriendlyConnection object](#the-friendlyconnection-object)-section.
  - When set to 'dbapi', the created connection will be a plain PG8000 DB-API Connection.
  - When set to 'sqlite3', a few (superficial) additions are added on top of the DB-API to make it resemble the Python `sqlite3` API more closely:
    - `Connection` objects have an `execute` method that creates a new cursor and 
        runs the given query on it.
    - Row objects can be indexed using numeric indexes as well as column names,
        just like (like with `connection.row_factory = sqlite3.Row` for `sqlite3`).
    - Autocommit mode is enabled by default.
    - Parameterized queries use `?` as a placeholder. (`paramstyle = 'qmark'`)
- `config` (Config | None): An object obtained through `get_config()` can be given to configure the connection. This causes `dirname` to be ignored.

Arguments:
- dirname (str, defaults to `data/postgresqlite`): The directory where the configuration file (`postgresqlite.json`) will be read or created, and where database files will be stored. If the path does not exist, it will be created.
- sqlite_compatible (bool, defaults to `True`): When set, a few (superficial) changes are made to the exposed DB-API to make it resemble the Python `sqlite3` API more closely, as described in the *Features* section.
- config (`Config` object, defaults to `None`): This can be an object returned by `get_config`. When `None`, the `connect` method will create a default configuration.

Returns a [DB-API compatible connection object](https://peps.python.org/pep-0249/#connection-objects).

#### `postgresqlite.get_config(dirname='data/postgresqlite')`

Start a server (if needed), wait for it, and return the config object. If the PostgreSQL server is in autostart mode (which is the default), it will be kept running until some time after the calling process (and any other processes that depend on this server) have terminated.
 
Arguments:
- dirname (str): The directory where the configuration file (`postgresqlite.json`) will be read or created, and where database files will be stored. If the path does not exist, it will be created.

Returns a `Config` object, that includes (among others) the following attributes:
- `user` (string): Database user name.
- `password` (string): Database password. (Can be `None`.)
- `port` (int): Database TCP port. (Can be `None`.)
- `host` (string): Database host name. (Can be `None`.)
- `database` (string): Database name.
- `socket` (string): Database UNIX domain socket. (Can be `None`.)
- `env` (dict): A dictionary containing `PGHOST`, `PGPORT`, `PGDATABASE`, `PGSOCKET`, `PGUSER`, `PGPASSWORD` and `PGURI` keys with their appropriate values.

#### `postgresqlite.get_uri(dirname='data/postgresqlite', driver='pg8000')`
Start a server (if needed), wait for it, and return a connection URL.

Arguments:
- dirname (str): The directory where the configuration file (`postgresqlite.json`) will be read or created, and where database files will be stored. If the path does not exist, it will be created.
- driver (str): The URI may include a driver part (`postgresql+DRIVER://user:pwd@host/db`), which we'll set to `pg8000` by default. This parameter allows you to specify a different direct, or leave it out (by providing `None`).

### The `FriendlyConnection` object

By default the `postgresqlite.connect` method will return a `FriendlyConnection`, which is a DB-API compatible `Connection` object, but with a few programmer-friendly differences:

- Row objects can be indexed using column names as well as column indexes. For example `row['name']` instead of `row[0]`. In addition, attribute syntax can be used, like `row.name`.
- Autocommit mode is enabled by default.
- Query parameters are in `:named` style.
- Exceptions are instances of `SQLError` and contain easy to read error messages, highlighting the offending part of the query.

Besides, those changes, the `FriendlyConnection` object offers a couple of additional methods:

#### execute(sql: str, params: dict = {})

Create a new `Cursor`, execute the given `sql` with the given `params` dictionary on that cursor, and return the `Cursor`. Example:

```python
row = db.execute('select * from test where id=:id', {'id': 123})).fetchone()`
```

#### query(sql, param1=.., param2=..)

Execute the `sql` with the given parameters, returning a list of `FriendlyRow` objects. Example:

```python
rows = db.query('select id, name from test where name=:name', name='Ivo')
print(len(rows), rows[0].id) # 3 123
```

#### query_row(sql, param1=.., param2=..)

Execute the `sql` with the given parameters, returning a single `FriendlyRow` object or `None` (if no rows were returned). If the query results in more than one row, an exception is thrown. Example:

```python
row = db.query_row('select id, name from test where id=:id', id=123)
print(row.id, row.name) # 123 Ivo
```

#### query_column(sql, param1=.., param2=..)

Execute the `sql` with the given parameters, returning a list of values. If the query results in more than one column, an exception is thrown. Example:

```python
test_ids = db.query_column('select id from test where name=:name', name='Ivo')
print(test_ids) # [123, 456, 789]
```

#### query_value(sql, param1=.., param2=..)

Execute the `sql` with the given parameters, returning a single value. If the query results in more than one column or more than one row, an exception is thrown. Example:

```python
test_id = db.query_value('select id from test where name=:name limit 1', name='Ivo')
print(test_id) # 123
```


### CLI interface

In order to easily access the database your application is using, PostgreSQLite can open a client application (such as *psql* or [Beekeeper Studio](https://www.beekeeperstudio.io/)) for you, making sure the database is started (and doesn't shutdown) while the application is open, and passing in connection details.

The connection details are provided in environment variables called `PGHOST`, `PGPORT`, `PGDATABASE`, `PGSOCKET`, `PGUSER`, `PGPASSWORD` and `PGURI`. 

An application can be started by running the `postgresqlite` package with `-m`, providing the executable and its arguments as arguments.
- When no arguments are passed: `psql`, the default PostgreSQL client, is run. So opening your database in `psql` can be done like this:
  ```sh
  python -mpostgresqlite
  ```
  The `psql` client uses the environment variables to set up a connection.
- When exactly one argument is passed and it contains the string `$PG`, it will be executed as a shell command. So in order to pass the URI as the argument to an application, one could use:
  ```sh
  python -mpostgresqlite 'xdg-open $PGURI'
  ```
  As Beekeeper Studio registers as an opener for the `postgresql://` protocol, the above command can be used to open your database with it.
- Otherwise, the arguments are used to launch the application as-is. It can use the environment variables to set up a connection.
  ```sh
  python -mpostgresqlite my-client connect --extra-fancy
  ```

Optionally, the directory where the configuration file (`postgresqlite.json`) will be read or created, and where database files will be stored, can be specified by prefixing the command with `-c` `<dirname>`. For example:

```sh
python -mpostgresqlite -d mydbdir 'xdg-open $PGURI'
```


### The config file

When PostgreSQLite is first started (for a certain directory), a configuration file called `postgresqlite.json` is created. After initial creation, the file is all yours to modify. It contains the following values:

- `autostart` (bool): When true, PostgreSQLite will automatically start/stop a PostgreSQL server. Otherwise, it will just connect to an existing database given the credentials in this file.
- `host` (str): Connection host name.
- `port` (int): Connection TCP port.
- `socket` (str): Connection UNIX domain socket (optional).
- `database` (int): Database name.
- `user` (str): Connection user name.
- `password` (str): Connection password name (optional).
- `socket_id` (str): A (random) string that distinguishes this instance of PostgreSQLite, for creating the PostgreSQL unix socket file in `/tmp`.

These fields are only relevant when `autostart` is true:
- `expire_seconds` (int): The time in seconds after which a server is shutdown when there are no active clients anymore.
- `pg_cache_dir` (str): The directory into which PostgreSQL will be installed and from which it will be ran.
- `postgresql_version` (str): The version of PostgreSQL to use. Currently only `"14.3"` is supported.


### The autostart mechanism

PostgreSQLite uses a series of lock files (stored in `locks/` in the configuration directory) to coordinate different application instances that may want to use the database. When such a process starts and no database is running, it forks a daemon that will start and eventually stop the PostgreSQL server. Stopping happens when there have been no more database applications running for `expire_seconds`, based on the lock files created by each application.

In case you want to force the server to stop immediately, you can do so by deleting the `locks/daemon.lock` file within the configuration directory. Starting the application again will cause the server to start anew.


### Standalone PostgreSQL

The autostart feature installs PostgreSQL from a tarball stored as a [GitHub release](https://github.com/vanviegen/postgresqlite/releases/tag/libs). This tarball is created using the `Dockerfile` and `run.sh` script provided in the `create-standalone-postgresql/` directory.

It works by copying the PostgreSQL binaries and depend files (including any `.so` files being used) from an Arch linux installation to a `.tar.gz` file. 
