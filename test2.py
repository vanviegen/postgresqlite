import postgresqlite
from time import sleep

db1 = postgresqlite.connect()

db1.query("drop table if exists test")
db1.query("create table test(id serial not null, name text not null)")
while True:
    print("ok")
    sleep(1)
    