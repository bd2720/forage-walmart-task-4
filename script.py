import sqlite3
# open connection to DB
dbTestName = "shipment_database.TEST.db"
dbName = "shipment_database.db"
dbConnection = sqlite3.connect(dbName)
# create cursor
dbCursor = dbConnection.cursor()
# query master table to see what tables exist
tables = dbCursor.execute("SELECT name FROM sqlite_master")
tables = tables.fetchall()
print(tables)
# display contents of each table
for tableName in tables:
  # skip table if created by sqlite
  if tableName[0][0:7] == "sqlite_":
    continue
  res = dbCursor.execute("SELECT * from %s" % tableName)
  print(tableName)
  print(res.fetchall())

# query from first db
# close connection
dbConnection.close()
