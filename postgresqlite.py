import os, string, random, json, sys, subprocess, fcntl, time, traceback, socket
from collections import namedtuple

def connect(dirname="data/postgresqlite"):
    """Start a server (if needed), wait for it, and return a dbapi compatible object.
    This requires pg8000 to be installed."""
    import pg8000.dbapi

    pg8000.dbapi.paramstyle = "qmark"

    pg8000.dbapi.Connection.execute = _conn_execute
    pg8000.dbapi.Cursor._fetchone = pg8000.dbapi.Cursor.fetchone
    pg8000.dbapi.Cursor.fetchall = _cursor_fetchall
    pg8000.dbapi.Cursor.fetchone = _cursor_fetchone

    config = get_config(dirname)
    return pg8000.dbapi.connect(user=config.user, password=config.password, unix_sock=_get_socket(config))


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
    """Start a server (if needed), wait for it, and return a connection URL."""
    config = get_config(dirname)
    return f"postgresql{'+'+driver if driver else ''}://{config.user}:{config.password}@localhost:{config.port}/{config.database}"


def _get_socket(config):
    return f"{config.directory}/.s.PGSQL.{config.port}"


def get_config(dirname="data/postgresqlite"):
    """Start a server (if needed), wait for it, and return the config object."""
    os.makedirs(dirname, exist_ok=True)

    config = _load_config(dirname)

    if config.autostart:
        _auto_start(config)

    count = 0
    while True:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(_get_socket(config))
                break
        except FileNotFoundError:
            pass
        count += 1
        if count==3:
            print("Waiting", end="", flush=True)
        elif count>3:
            print(".", end="", flush=True)
        time.sleep(0.5)
    if count>=3:
        print()

    return config


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
        log_fd = open(config.directory+"/postgresqlite.log", "w")

        if not os.path.exists(config.directory+"/pgdata/postgresql.conf"):
            print("Initializing new PostgreSQL data directory..", file=sys.stderr)
            password_file = config.directory+"/password.txt"
            with open(password_file, "w") as file:
                file.write(config.password)
            
            subprocess.run([
                config.autostart_initdb,
                '-D', config.directory+"/pgdata",
                '-U', config.user,
                f'--pwfile={password_file}'
            ], env=os.environ, stderr=log_fd, stdout=log_fd)

            os.remove(password_file)

        print("Starting PostgreSQL..", file=sys.stderr)
        _run_as_daemon(lambda: _run_server(daemon_fd, log_fd, config))

    client_file = lockdir + "/" + _make_random_word()
    client_fd = open(client_file, "w")
    fcntl.flock(client_fd, fcntl.LOCK_EX)


def _make_random_word(length=12):
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


def _load_config(dirname):
    config_file = dirname+"/postgresqlite.json"
    try:
        with open(config_file) as file:
            config = json.load(file)
    except FileNotFoundError:
        config = {
            "autostart": True,
            "autostart_postgres": "/usr/sbin/postgres",
            "autostart_initdb": "/usr/sbin/initdb",
            "autostart_expire_seconds": 300,
            "directory": dirname,
            "user": "postgres",
            "password": _make_random_word(),
            "port": random.randint(32768,60999),
            "host": "localhost",
            "database": "postgres",
        }
        with open(config_file, "w") as file:
            json.dump(config, file)

    # Create full paths (before we may daemonize)
    for name in ["directory"]:
        if name in config:
            config[name] = os.path.realpath(config[name])

    return namedtuple("Config", config.keys())(*config.values())


def _run_server(daemon_fd, log_fd, config):

    lockdir = config.directory+"/locks"

    proc = subprocess.Popen([
        config.autostart_postgres,
        "-D", config.directory+"/pgdata",
        "-p", str(config.port),
        f"--unix_socket_directories={config.directory}"
    ], env=os.environ, stderr=log_fd, stdout=log_fd)

    try:
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
                print("PostgreSQLite shutting down server as {lockdir}/daemon.lock has gone...")
                break

            if client_count == 0:
                no_client_time += 1
                if no_client_time >= config.autostart_expire_seconds:
                    print(f"PostgreSQLite shutting down idle server...", file=log_fd, flush=True)
                    break
            else:
                no_client_time = 0

            if client_count != last_client_count:
                print(f"PostgreSQLite now has {client_count} client(s)", file=log_fd, flush=True)
                last_client_count = client_count

            time.sleep(1)
    except Exception as e:
        print(traceback.format_exc(), file=log_fd, flush=True)
        print(f"PostgreSQLite shutting down server due to error...", file=log_fd, flush=True)

    try:
        os.remove(lockdir + "/daemon.lock")
    except FileNotFoundError:
        pass
    daemon_fd.close()

    proc.terminate()
    try:
        proc.wait(10)
        print(f"PostgreSQLite shutdown successfull", file=log_fd, flush=True)
    except TimeoutExpired:
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


if __name__ == "__main__":
    config = get_config()

    if len(sys.argv)==2 and sys.argv[1] == "psql":
        subprocess.run([
            "psql",
            "-h", config.directory,
            "-p", str(config.port),
            "-U", config.user,
        ], env=os.environ.update({"PGPASSWORD": config.password}))
    else:
        url = f"postgresql://{config.user}:{config.password}@localhost:{config.port}/{config.database}"
        print(f"Opening client for {url}...")
        subprocess.run(["xdg-open", url], env=os.environ)
