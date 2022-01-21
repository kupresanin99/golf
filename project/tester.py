import sqlite3
db_path = "./data/golf.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute("SELECT DISTINCT tourn_id FROM tournament WHERE format = 'Stroke'")

tourn_ids = cur.fetchall()
print(type(rows))
for row in rows:
  print(str(row[0]))