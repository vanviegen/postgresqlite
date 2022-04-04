import os, string, random, json, sys, subprocess, fcntl, time, traceback, socket, urllib.request, tarfile


def connect(dirname="data/postgresqlite", sqlite_compatible=True):
    """Start a server (if needed), wait for it, and return a dbapi compatible object.
    
    Args:
        dirname (str): The directory where the configuration file (`postgresqlite.json`)
            will be read or created, and where database files will be stored. If the
            path does not exist, it will be created.
        sqlite_compatible (bool): When set, a few (superficial) changes are made to the
            exposed DB-API to make it resemble the Python `sqlite3` API more closely.
            The README provides more details.
    """
    import pg8000.dbapi

    if sqlite_compatible:
        pg8000.dbapi.paramstyle = "qmark"

        pg8000.dbapi.Connection.execute = _conn_execute
        pg8000.dbapi.Cursor._fetchone = pg8000.dbapi.Cursor.fetchone
        pg8000.dbapi.Cursor.fetchall = _cursor_fetchall
        pg8000.dbapi.Cursor.fetchone = _cursor_fetchone

    config = get_config(dirname)
    connection = pg8000.dbapi.connect(user=config.user, password=config.password, unix_sock=config.socket)
    if sqlite_compatible:
        connection.autocommit = True
    return connection


def _conn_execute(self, query, *args):
    cursor = self.cursor()
    cursor.execute(query, *args)
    return cursor


def _create_lookup_dict(descr):
    return {info[0]: index for index, info in enumerate(descr)}
        

class DictRow(list):
    def __init__(self, row, lookup):
        super().__init__(row)
        self._lookup = lookup

    def __getitem__(self,key):
        if type(key)==str:
            key = self._lookup[key]
        return super().__getitem__(key)

    def __str__(self):
        return str({key: list.__getitem__(self, index) for key,index in self._lookup.items()})


def _cursor_fetchone(self):
    lookup = _create_lookup_dict(self.description)
    row = self._fetchone()
    if row:
        return DictRow(row, lookup)


def _cursor_fetchall(self):
    results = []
    lookup = _create_lookup_dict(self.description)
    while True:
        result = self._fetchone()
        if result==None:
            return results
        results.append(DictRow(result,lookup))


def get_uri(dirname="data/postgresqlite", driver="pg8000"):
    """Start a server (if needed), wait for it, and return a connection URL.    
    Args:
        dirname (str): The directory where the configuration file (`postgresqlite.json`)
            will be read or created, and where database files will be stored. If the
            path does not exist, it will be created.
        driver (str): The URI may include a driver part (`postgresql+DRIVER://user:pwd@host/db`),
            which we'll set to `pg8000` by default. This parameter allows you to specify a 
            different direct, or leave it out (by providing `None`).
    """
    get_config(dirname).get_uri(driver)


def get_config(dirname="data/postgresqlite"):
    """Start a server (if needed), wait for it, and return the config object. If the 
    PostgreSQL server is in autostart mode (which is the default), it will be kept
    running until some time after the calling process (and any other processes that
    depend on this server) have terminated.
    
    Args:
        dirname (str): The directory where the configuration file (`postgresqlite.json`)
            will be read or created, and where database files will be stored. If the
            path does not exist, it will be created.

    """
    os.makedirs(dirname, exist_ok=True)

    config = Config(dirname)

    if config.autostart:
        _auto_start(config)

    count = 0
    while True:
        if not os.path.exists(config.expand_path(config.directory+"/locks/daemon.lock")):
            print(f"\nPostgreSQL server failed to start - check {config.directory}/postgresqlite.log", file=sys.stderr)
            exit(1)
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(config.socket)
                break
        except FileNotFoundError:
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
    if os.path.exists(config.expand_path(config.postgres_bin)):
        return
    bin_dir = config.expand_path(config.bin_directory)
    os.makedirs(bin_dir, exist_ok=True)

    print(f"Downloading PostgreSQL {config.postgresql_version}")

    url = f"https://github.com/vanviegen/postgresqlite/releases/download/libs/standalone-postgresql-{config.postgresql_version}.tar.gz"
    url_stream = urllib.request.urlopen(url)
    tar_stream = tarfile.open(fileobj=url_stream, mode="r|gz")
    tar_stream.extractall(path=bin_dir)


def _auto_start(config):
    lockdir = config.directory+"/locks"
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

        log_fd = open(config.directory+"/postgresqlite.log", "w")

        if not os.path.exists(config.directory+"/pgdata/postgresql.conf"):
            print("Initializing new PostgreSQL data directory..", file=sys.stderr)
            password_file = config.directory+"/password.txt"
            with open(password_file, "w") as file:
                file.write(config.password)
            
            subprocess.run([
                config.expand_path(config.initdb_bin),
                '-D', config.directory+"/pgdata",
                '-U', config.user,
                f'--pwfile={password_file}'
            ], env=dict(os.environ, LD_LIBRARY_PATH=config.expand_path(config.bin_directory)+"/system-lib"), check=True, stderr=log_fd, stdout=log_fd)

            os.remove(password_file)

        print("Starting PostgreSQL..", file=sys.stderr)
        _run_as_daemon(lambda: _run_server(daemon_fd, log_fd, config))

    client_file = lockdir + "/" + _make_random_word()
    client_fd = open(client_file, "w")
    fcntl.flock(client_fd, fcntl.LOCK_EX)


def _make_random_word(length=12):
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


class Config:
    def __init__(self, directory=None):
        if directory==None:
            directory = "data/postgresqlite"

        self.autostart = True
        self.expire_seconds = 180
        self.bin_cache_directory = "~/.cache/postgresqlite"
        self.postgresql_version = "13.6"
        self.user = "postgres"
        self.password = _make_random_word()
        self.port = random.randint(32768,60999)
        self.host = "localhost"
        self.database = "postgres"

        config_file = directory+"/postgresqlite.json"
        try:
            with open(config_file) as file:
                for key,val in json.load(file).items():
                    setattr(self, key, val)
        except FileNotFoundError:
            print(f"Creating new configuration at {config_file}..", file=sys.stderr)
            with open(config_file, "w") as file:
                json.dump(self.__dict__, file)

        # Create full paths (before we may daemonize)
        self.directory = os.path.realpath(directory)

    def __getattr__(self, name):
        if "get_"+name not in dir(self):
            raise AttributeError(name)
        return getattr(self, "get_"+name)()

    def get_bin_directory(self):
        return f"{self.bin_cache_directory}/{self.postgresql_version}"

    def get_postgres_bin(self):
        return f"{self.bin_directory}/bin/postgres"

    def get_initdb_bin(self):
        return f"{self.bin_directory}/bin/initdb"

    def expand_path(self, path):
        return os.path.normpath(os.path.join(self.directory, os.path.expanduser(path)))    

    def get_socket(config):
        if config.autostart:
            return config.expand_path(f".s.PGSQL.{config.port}")

    def get_uri(config, driver=None):
        return f"postgresql{'+'+driver if driver else ''}://{config.user}:{config.password}@localhost:{config.port}/{config.database}"



def _run_server(daemon_fd, log_fd, config):

    lockdir = config.directory+"/locks"
    proc = None

    try:
        proc = subprocess.Popen([
            config.expand_path(config.postgres_bin),
            "-c", f"dynamic_library_path={config.expand_path(config.bin_directory)}/lib",
            "-D", config.directory+"/pgdata",
            "-p", str(config.port),
            f"--unix_socket_directories={config.directory}"
        ], env=dict(os.environ, LD_LIBRARY_PATH=config.expand_path(config.bin_directory)+"/system-lib"), stderr=log_fd, stdout=log_fd)

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
                    client_count += 1 
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB) # Non blocking
                        os.unlink(pathname) # File no longer locked
                        print(f"PostgreSQLite removed {pathname}", file=log_fd, flush=True)

                    except OSError:
                        pass # File is still locked

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

    if proc:
        proc.terminate()
        try:
            proc.wait(10)
            print(f"PostgreSQLite shutdown successfull", file=log_fd, flush=True)
        except subprocess.TimeoutExpired:
            print(f"PostgreSQLite killing server", file=log_fd, flush=True)
            proc.kill()
            proc.wait()

    log_fd.close()


def _run_as_daemon(daemon_callback):
    # Do the Unix double-fork magic; see Stevens's book "Advanced
    # Programming in the UNIX Environment" (Addison-Wesley) for details
    try:
        pid = os.fork()
        if pid > 0:
            # Return the original process
            return
    except OSError as e:
        print(f"fork #1 failed: {e.errno} ({e.strerror})", file=sys.stderr)
        sys.exit(1)

    # Decouple from parent environment
    os.chdir("/")
    os.setsid(  )
    os.umask(0)

    # Do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # Exit from second parent; print eventual PID before exiting
            sys.exit(0)
    except OSError as e:
        print(f"fork #2 failed: {e.errno} (e.strerror)", file=sys.stderr)
        sys.exit(1)

    daemon_callback()
    sys.exit(0)

