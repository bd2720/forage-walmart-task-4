import sqlite3
# open connection to DB
dbName = 'shipment_database.TEST.db'
dbConnection = sqlite3.connect(dbName)
# create cursor
dbCursor = dbConnection.cursor()
# insert into products table
dbTable = 'product'
prodID = 65
prodName = 'bikes'
dbCursor.execute('INSERT INTO %s VALUES(?, ?)'%dbTable, (prodID, prodName))
# commit
dbConnection.commit()
# query
queryResult = dbCursor.execute('SELECT * from ' + dbTable)
print(queryResult.fetchall())
# delete
dbCursor.execute('DELETE FROM ' + dbTable)
# commit
dbConnection.commit()
# query
queryResult = dbCursor.execute('SELECT * from ' + dbTable)
print(queryResult.fetchall())
# close connection
dbConnection.close()
