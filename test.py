import easypostgres, time

db = easypostgres.connect()

while True:
    rows = db.execute("select * from untitled_table where id=?", [1]).fetchall()
    for row in rows:
        print(row)
        print(row[0], row['created_at'])
    # print(type(db.execute("select * from untitled_table").fetchone()))
    # print(db.execute("select * from untitled_table").fetchone())
    time.sleep(1)