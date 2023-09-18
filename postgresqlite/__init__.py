import os, string, random, json, sys, subprocess, fcntl, time, traceback, socket, urllib.request, urllib.error, tarfile, pg8000, pg8000.dbapi, glob, re


class FriendlyConnection(pg8000.dbapi.Connection):
    def cursor(self):
        return FriendlyCursor(self)

    def execute(self, *args, **kwargs):
        cursor = self.cursor()
        cursor.execute(*args, **kwargs)
        return cursor

    def query(self, sql, **kwparams):
        cursor = self.cursor()
        result = cursor.query(sql, **kwparams)
        cursor.close()
        return result

    def query_row(self, sql, **kwparams):
        cursor = self.cursor()
        result = cursor.query_row(sql, **kwparams)
        cursor.close()
        return result

    def query_value(self, sql, **kwparams):
        cursor = self.cursor()
        result = cursor.query_value(sql, **kwparams)
        cursor.close()
        return result

    def query_column(self, sql, **kwparams):
        cursor = self.cursor()
        result = cursor.query_column(sql, **kwparams)
        cursor.close()
        return result


error_start_marker = "\u001b[31m"
error_end_marker = "\u001b[0m"

def get_exception_message(query, args, org_exception):
    msg = str(org_exception)
    
    match_m = re.search("'M': '((\\\\.|[^'])+)'", msg) # message
    if match_m:
        match_h = re.search("'H': '((\\\\.|[^'])+)'", msg) # hint
        match_p = re.search("'P': '((\\\\.|[^'])+)'", msg) # position
        msg = match_m.group(1)
        msg = msg[0].upper() + msg[1:] + "."
        if match_h:
            msg += f"\nHint: {match_h.group(1)}"
        if match_p and match_p.group(1).isdigit():
            # Make the error position in the query red
            start_pos = int(match_p.group(1))-1
            match_word = re.search('^[a-zA-Z_.]+|[^a-zA-Z_.]+', query[start_pos:])
            if match_word and match_word.group(0).strip():
                end_pos = start_pos + len(match_word.group(0))
            else:
                query = query[:start_pos] + "⚠" + query[start_pos:]
                end_pos = start_pos + 1
            query = query[0:start_pos] + error_start_marker  + query[start_pos:end_pos] + error_end_marker + query[end_pos:]

    msg = f"{msg}\nFor query:\n\t" + query.replace("\n","\n\t")
    if args:
        msg += f"\nWith arguments: {args}"
    return msg


class FriendlyCursor(pg8000.dbapi.Cursor):
    def execute(self, sql, params={}, **kwargs):
        self._lookup = None
        org_paramstyle = pg8000.dbapi.paramstyle
        pg8000.dbapi.paramstyle = self._c._paramstyle
        try:
            super().execute(sql, params, **kwargs)
            if self.description:
                self._lookup = {info[0]: index for index, info in enumerate(self.description)}
        except Exception as e:
            msg = get_exception_message(sql, params, e)
            raise type(e)(msg) from None
        finally:
            pg8000.dbapi.paramstyle = org_paramstyle

    def __next__(self):
        data = super().__next__()
        return FriendlyRow(data, self._lookup)

    def query(self, sql, **kwparams):
        self.execute(sql, kwparams)
        if self._lookup:
            return [row for row in self]

    def query_row(self, sql, **kwparams):
        self.execute(sql, kwparams)
        if not self._lookup:
            raise pg8000.dbapi.ProgrammingError("query should return data")
        if self.rowcount > 1:
            raise pg8000.dbapi.ProgrammingError("at most a single result row was expected")
        return self.fetchone()

    def query_value(self, sql, **kwparams):
        row = self.query_row(sql, **kwparams)
        if len(self.description) != 1:
            raise pg8000.dbapi.ProgrammingError("a single result column was expected")
        if row:
            return row[0]

    def query_column(self, sql, **kwparams):
        self.execute(sql, kwparams)
        if not self._lookup:
            raise pg8000.dbapi.ProgrammingError("query should return data")
        if len(self.description) != 1:
            raise pg8000.dbapi.ProgrammingError("a single result column was expected")
        return [row[0] for row in self]

    def __str__(self):
        return f"<FriendlyCursor rowcount={self.rowcount} columns={list(self._create_lookup_dict())}>"


class FriendlyRow:
    def __init__(self, data, lookup):
        self._data = data
        self._lookup = lookup

    def __getitem__(self, key):
        if type(key)==str:
            key = self._lookup[key]
        return self._data[key]

    def __getattr__(self, key):
        return self._data[self._lookup[key]]

    def __str__(self):
        return "<FriendlyRow " + ' '.join([str(key)+"="+repr(self._data[index]) for key,index in self._lookup.items()]) + ">"

    def keys(self):
        return self._lookup.keys()

    def __len__(self):
        return len(self._lookup)


def connect(dirname="data/postgresqlite", mode='friendly', config=None):
    """Start a server (if needed), wait for it, and return a dbapi-compatible object.
    
    Args:
        dirname (str): The dir where the configuration file (`postgresqlite.json`)
            will be read or created, and where database files will be stored. If the
            path does not exist, it will be created.
        mode ('easy', 'sqlite', 'dbapi'): When set to 'dbapi', the created connection
            will be a plain PG8000 DB-API Connection. When set to 'sqlite3', a few
            (superficial) additions are added on top of the DB-API to make it resemble the
            Python `sqlite3` API more closely:
            - `Connection` objects have an `execute` method that creates a new cursor and 
              runs the given query on it.
            - Row objects can be indexed using numeric indexes as well as column names,
              just like (like with `connection.row_factory = sqlite3.Row` for `sqlite3`).
            - Autocommit mode is enabled by default.
            - Parameterized queries use `?` as a placeholder. (`paramstyle = 'qmark'`)
            When the mode is set to `friendly` (the default):
            - All of the `sqlite3` additions mentioned above apply.
            - Parameterized queries use `:my_param`-style placeholders. (`paramstyle = 'named'`)
            - `Connection` and `Cursor` objects have `query`, `query_row`, `query_column` and
              `query_value` methods, as documented in the `README.md`.
        config (Config | None): An object obtained through `get_config()` can be given
            to configure the connection. This causes `dirname` to be ignored.
    """

    config = config or get_config(dirname)

    retries = 0
    while True:
        try:
            connection = pg8000.dbapi.connect(user=config.user, password=config.password, unix_sock=config.socket)
            connection.cursor().execute('SELECT 1')
        except Exception as e:
            if 'the database system is starting up' not in str(e) or retries >= 50:
                raise e
            retries += 1
            time.sleep(0.2)
            continue
        break

    # End the transaction started by `SELECT 1`
    connection.cursor().execute('COMMIT')

    if mode != 'dbapi':        
        connection.__class__ = FriendlyConnection
        connection.autocommit = True
        connection._paramstyle = "qmark" if mode == 'sqlite3' else 'named'

    return connection


def get_uri(dirname="data/postgresqlite", driver="pg8000"):
    """Start a server (if needed), wait for it, and return a connection URL.    
    Args:
        dirname (str): The dir where the configuration file (`postgresqlite.json`)
            will be read or created, and where database files will be stored. If the
            path does not exist, it will be created.
        driver (str): The URI may include a driver part (`postgresql+DRIVER://user:pwd@host/db`),
            which we'll set to `pg8000` by default. This parameter allows you to specify a 
            different direct, or leave it out (by providing `None`).
    """
    return get_config(dirname).get_uri(driver)


def get_config(dirname="data/postgresqlite"):
    """Start a server (if needed), wait for it, and return the config object. If the 
    PostgreSQL server is in autostart mode (which is the default), it will be kept
    running until some time after the calling process (and any other processes that
    depend on this server) have terminated.
    
    Args:
        dirname (str): The dir where the configuration file (`postgresqlite.json`)
            will be read or created, and where database files will be stored. If the
            path does not exist, it will be created.

    """
    os.makedirs(dirname, exist_ok=True)

    config = Config(dirname)

    if not config.autostart:
        return config

    _auto_start(config)

    count = 0
    while True:
        if not os.path.exists(config.expand_path(config.dir+"/locks/daemon.lock")):
            print(f"\nPostgreSQL server failed to start - check {config.dir}/postgresqlite.log", file=sys.stderr)
            exit(1)
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(config.socket)
                break
        except (FileNotFoundError, ConnectionRefusedError):
            pass
        count += 1
        if count==3:
            print("Waiting", end="", flush=True, file=sys.stderr)
        elif count>3:
            print(".", end="", flush=True, file=sys.stderr)
        time.sleep(0.5)
    if count>=3:
        print(file=sys.stderr)

    return config


def _download_server(config):
    if os.path.exists(config.exp_postgres_bin):
        return
    os.makedirs(config.exp_pg_dir, exist_ok=True)

    print(f"Downloading PostgreSQL {config.postgresql_version}..", file=sys.stderr)

    url = f"https://github.com/vanviegen/postgresqlite/releases/download/libs/standalone-postgresql-{config.postgresql_version}-{os.uname().sysname}-{os.uname().machine}.tar.gz"
    try:
        url_stream = urllib.request.urlopen(url)
        tar_stream = tarfile.open(fileobj=url_stream, mode="r|gz")
        tar_stream.extractall(path=config.exp_pg_dir)
    except urllib.error.HTTPError as err:
        print(f"Failed to download {url}: {err}")

    if not os.path.exists(config.exp_postgres_bin):
        print(f"Download and extract to {config.exp_pg_dir} failed.", file=sys.stderr)
        sys.exit(1)


def _auto_start(config):
    lockdir = config.dir+"/locks"
    os.makedirs(lockdir, exist_ok=True)

    daemon_file = lockdir + "/daemon.lock"
    daemon_fd = open(daemon_file, "a")
    try:
        fcntl.flock(daemon_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        daemon_fd.close()
        daemon_fd = None

    if daemon_fd is None:
        print("Connecting to already running PostgreSQL instance..", file=sys.stderr)
    else:
        _download_server(config)

        log_fd = open(config.dir+"/postgresqlite.log", "a", buffering=1)

        if not os.path.exists(config.dir+"/pgdata/postgresql.conf"):
            print("Initializing new PostgreSQL data dir..", file=sys.stderr)
            password_file = config.dir+"/password.txt"
            with open(password_file, "w") as file:
                file.write(config.password)
            
            subprocess.run([
                config.exp_initdb_bin,
                '-D', config.dir+"/pgdata",
                '-U', config.user,
                f'--pwfile={password_file}'
            ], check=True, stderr=log_fd, stdout=log_fd)

            os.remove(password_file)

        print("Starting PostgreSQL..", file=sys.stderr)
        _run_as_daemon(lambda: _run_server(daemon_fd, log_fd, config), keep_fds={daemon_fd,log_fd}, change_cwd=False)

    client_file = lockdir + "/" + _make_random_word()
    global client_fd # to make sure the GC doesn't close our file
    client_fd = open(client_file, "w")
    fcntl.flock(client_fd, fcntl.LOCK_EX)


def _make_random_word(length=12):
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


class Config:
    def __init__(self, dir=None):
        if dir==None:
            dir = "data/postgresqlite"

        self.autostart = True
        self.expire_seconds = 180
        self.pg_cache_dir = "~/.cache/postgresqlite"
        self.postgresql_version = "14.3"
        self.user = "postgres"
        self.password = _make_random_word()
        self.port = random.randint(32768,60999)
        self.host = "localhost"
        self.database = "postgres"
        self.socket_id = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=8))

        config_file = dir+"/postgresqlite.json"
        try:
            with open(config_file) as file:
                for key,val in json.load(file).items():
                    setattr(self, key, val)
        except FileNotFoundError:
            print(f"Creating new configuration at {config_file}..", file=sys.stderr)
        with open(config_file, "w") as file:
            json.dump(self.__dict__, file)

        # Create full paths (before we may daemonize)
        self.dir = os.path.realpath(dir)

    def __getattr__(self, name):
        if "get_"+name in dir(self):
            return getattr(self, "get_"+name)()
        if name.startswith("exp_") and "get_"+name[4:] in dir(self):
            return self.expand_path(getattr(self, "get_"+name[4:])())
        raise AttributeError(name)

    def get_pg_dir(self):
        return f"{self.pg_cache_dir}/{self.postgresql_version}"

    def get_postgres_bin(self):
        return f"{self.pg_dir}/bin/postgres"

    def get_initdb_bin(self):
        return f"{self.pg_dir}/bin/initdb"

    def expand_path(self, path):
        return os.path.normpath(os.path.join(self.dir, os.path.expanduser(path)))

    def get_socket_dir(self):
        if self.autostart:
            # Placing the socket in de postgresqlite data dir may cause problems, as the path name
            # may exceed 107 characters.
            return f"/tmp/postgresqlite-{self.socket_id}"

    def get_socket(self):
        if self.autostart:
            return f"{self.socket_dir}/.s.PGSQL.{self.port}"

    def get_uri(config, driver=None):
        return f"postgresql{'+'+driver if driver else ''}://{config.user}:{config.password}@localhost:{config.port}/{config.database}"

    def get_env(self):
        env = dict(
            PGHOST = self.host,
            PGPORT = self.port,
            PGSOCKET = self.socket,
            PGDATABASE = self.database,
            PGUSER = self.user,
            PGPASSWORD = self.password,
            PGURI = self.uri,
        )
        for k,v in env.items():
            env[k] = '' if v==None else str(v)
        return env


def _run_server(daemon_fd, log_fd, config):
    sys.stdout = sys.stderr = log_fd
    print("PostgreSQL daemon is starting..")

    lockdir = config.dir+"/locks"
    proc = None
    
    os.makedirs(config.socket_dir, exist_ok=True)

    try:
        proc = subprocess.Popen([
            config.exp_postgres_bin,
            "-c", f"dynamic_library_path={config.exp_pg_dir}/lib",
            "-D", config.dir+"/pgdata",
            "-p", str(config.port),
            f"--unix_socket_directories={config.socket_dir}"
        ], stderr=log_fd, stdout=log_fd)

        no_client_time = 0
        last_client_count = -1
        while True:
            client_count = 0
            locked_by_me = False
            for filename in os.listdir(lockdir):
                pathname = lockdir + "/" + filename
                if filename == "daemon.lock":
                    if os.stat(pathname).st_ino == os.fstat(daemon_fd.fileno()).st_ino:
                        locked_by_me = True
                    continue
                with open(pathname) as fd:
                    try:
                        time.sleep(0.2) # Give the creator of the file a chance to lock before we do
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB) # Non blocking
                        os.unlink(pathname) # File no longer locked
                        print(f"PostgreSQLite removed {pathname} as it was no longer locked", file=log_fd, flush=True)

                    except OSError:
                        pass # File is still locked
                        client_count += 1

            if not locked_by_me:
                print("PostgreSQLite shutting down server as {lockdir}/daemon.lock has gone...", file=log_fd, flush=True)
                break

            if client_count == 0:
                no_client_time += 1
                if no_client_time >= config.expire_seconds:
                    print(f"PostgreSQLite shutting down idle server...", file=log_fd, flush=True)
                    break
            else:
                no_client_time = 0

            if client_count != last_client_count:
                print(f"PostgreSQLite now has {client_count} client(s)", file=log_fd, flush=True)
                last_client_count = client_count

            if proc.poll() != None:
                print(f"PostgreSQL terminated unexpectedly", file=log_fd, flush=True)
                break

            time.sleep(1)
    except Exception as e:
        print(traceback.format_exc(), file=log_fd, flush=True)
        print(f"PostgreSQLite shutting down server due to error...", file=log_fd, flush=True)

    try:
        os.remove(lockdir + "/daemon.lock")
    except FileNotFoundError:
        pass
    daemon_fd.close()

    if proc != None:
        proc.terminate()
        try:
            proc.wait(10)
            print(f"PostgreSQLite shutdown successful", file=log_fd, flush=True)
        except subprocess.TimeoutExpired:
            print(f"PostgreSQLite killing server", file=log_fd, flush=True)
            proc.kill()
            proc.wait()

    try:
        for filename in glob.glob(config.socket_dir + "/.s.PGSQL.*"):
            os.unlink(filename)
        os.rmdir(config.socket_dir)
    except Exception as e:
        print(f"Couldn't delete {config.socket_dir}", e, file=log_fd, flush=True)

    log_fd.close()


def _run_as_daemon(daemon_callback, keep_fds=set(), change_cwd=True):
    # Do the Unix double-fork magic; see Stevens's book "Advanced
    # Programming in the UNIX Environment" (Addison-Wesley) for details
    pid = os.fork()
    if pid > 0:
        # Return the original process
        return

    # Decouple from parent environment
    if change_cwd:
        os.chdir("/")
    os.setsid()
    os.umask(0)

    keep_fds = {fd if type(fd)==int else fd.fileno() for fd in keep_fds}
    for file in [sys.stdout, sys.stderr, sys.stdin]:
        try:
            if file.fileno() not in keep_fds:
                file.close()
        except:
            pass
    for fd in range(1024):
        if fd not in keep_fds:
            try:
                os.close(fd)
            except:
                pass

    # Do second fork
    pid = os.fork()
    if pid > 0:
        # Exit from second parent; print eventual PID before exiting
        sys.exit(0)

    try:
        daemon_callback()
    except Exception as e:
        # Make sure an exception is not caught by _run_as_daemon's caller
        print(traceback.format_exc(), file=sys.stderr, flush=True)
    sys.exit(0)


def main():
    directory = None
    cmd = sys.argv[1:]
    if len(cmd) >= 2 and cmd[0]=="-d":
        config = get_config(cmd[1])
        cmd = cmd[2:]
    else:
        config = get_config()
    cmd = cmd or ["psql"]

    if len(cmd) == 1 and '$PG' in cmd[0]:
        cmd = ["sh", "-c", cmd[0]]

    env = dict(os.environ)
    env.update(config.env)
    subprocess.run(cmd, env=env)
