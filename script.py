import sqlite3
import csv

dbName = 'shipment_database.db'

### EXTRACT PRODUCT DATA ###

# maps/assigns product names to product ID (sequential)
def initProdIDs(filepath, colIndex, idMap, productID):
  with open(filepath, newline='') as data:
    dataReader = csv.reader(data, delimiter=',')
    # read products
    for row in dataReader:
      productName = row[colIndex]
      if productName not in idMap:
        idMap[productName] = productID
        productID += 1
  return productID

productID = 0 # sequential ID for products
productDict = dict() # maps prodName to prodID
# call for data_0 and data_1, since both have "product"
productID = initProdIDs('data/shipping_data_0.csv', 2, productDict, productID)
productID = initProdIDs('data/shipping_data_1.csv', 1, productDict, productID)

### FORMAT PRODUCT DATA ###

# want a list of (prodID, name)
productTuples = [(productDict[name], name) for name in productDict]

### INSERT PRODUCT DATA ###

# connect to DB
dbConnection = sqlite3.connect(dbName)
# create cursor to execute queries
dbCursor = dbConnection.cursor()
# insert productTuples into product table
dbCursor.executemany("INSERT INTO product VALUES(?, ?)", productTuples)
# commit transaction
dbConnection.commit()
# close connection
dbConnection.close()