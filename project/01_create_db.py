import sqlite3
db_path = "./data/golf.db"
conn = sqlite3.connect(db_path)
conn.close()
