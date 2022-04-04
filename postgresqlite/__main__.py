import subprocess, sys, os
from . import  get_config

directory = None
cmd = sys.argv[1:]
if len(cmd) >= 2 and cmd[0]=="-d":
    config = get_config(cmd[1])
    cmd = cmd[2:]
else:
    config = get_config()
cmd = cmd or ["psql"]

env = dict(os.environ,
    PGHOST = config.host,
    PGPORT = config.port,
    PGSOCKET = config.socket,
    PGDATABASE = config.database,
    PGUSER = config.user,
    PGPASSWORD = config.password,
    PGURI = config.uri,
)
for k,v in env.items():
    env[k] = '' if v==None else str(v)
if len(cmd) == 1 and '$PG' in cmd[0]:
    cmd = ["sh", "-c", cmd[0]]

subprocess.run(cmd, env=env)
