import postgresqlite, os
from time import sleep

db1 = postgresqlite.connect()

db1.query("drop table if exists test")
db1.query("create table test(id serial not null, name text not null)")
db1.query("insert into test(name) values('Frank')")
print(db1.query("select * from test"))

print(db1.query_row("select * from test where id=:id", id=-1), "should be None")

print(db1.query_row("select * from test where id=:id", id=1))

print(db1.query_row("insert into test(name) values(:name) returning *", name='Piet'))

print(dict(db1.query_row("select name as _lookup from test limit :limit", limit=1)))

print(dict(db1.query_row("select * from test limit :limit", limit=1)))

print(len(db1.query_row("select * from test limit :limit", limit=1)))

print(db1.query_column("select id from test where name=:name", name='Piet'))

print(db1.query_value("select name from test where id=:id", id=1))

db2 = postgresqlite.connect(mode='sqlite3')

db2.execute('select id, name from test where name=?', ('frank',))

print(db1.query_value("select name from test where id=:id", id=1))

db1.close()
db2.close()

if os.fork():
    db1 = postgresqlite.connect()

    print("db1 select")
    db1.query('begin transaction isolation level serializable')
    row = db1.query_row("select * from test where name = 'Frank'")

    sleep(1)

    print("db1 update & commit")
    db1.query("update test set name = 'Frank1' where name = 'Frank'", id=row.id, name=row.name+'1')
    db1.query('commit')
    
    sleep(0.5)
    
    print("db1", db1.query_row("select * from test where id=:id", id=row.id))
else:
    db2 = postgresqlite.connect()

    sleep(0.5)

    print("db3 update & commit")
    row = db2.query_row("update test set name = 'Frank2' where name = 'Frank' returning *")

    sleep(1)

    print("db3", db2.query_row("select * from test where id=:id", id=row.id))
