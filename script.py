import sqlite3
import csv

dbName = 'shipment_database.db'
error = ""

### EXTRACT PRODUCT DATA ###

# maps/assigns product names to product ID (sequential)
def initProdIDs(filepath, colIndex, idMap, productID):
  with open(filepath, newline='') as data:
    dataReader = csv.reader(data, delimiter=',')
    # skip first line
    next(data)
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

### EXTRACT SHIPMENT DATA ###

# {shipment_id: (orig_id, dest_id, {prodName:prodCount}, newID)}
shipmentDict = dict()
# read from data_2
with open('data/shipping_data_2.csv', newline='') as data:
  dataReader = csv.reader(data, delimiter=',')
  # skip first line
  next(data)
  for row in dataReader:
    shipmentID = row[0]
    if shipmentID not in shipmentDict:
      shipmentDict[shipmentID] = (row[1], row[2], dict())
# update nested product dictionaries using data_1
with open('data/shipping_data_1.csv', newline='') as data:
  dataReader = csv.reader(data, delimiter=',')
  # skip first line
  next(data)
  for row in dataReader:
    shipmentID = row[0]
    prodName = row[1]
    if shipmentID not in shipmentDict:
      error = "data_1 contained a shipment_id not seen in data_2"
      break
    shipment = shipmentDict[shipmentID]
    shipmentProducts = shipment[2]
    # add/update product count in nested dict
    if prodName not in shipmentProducts:
      shipmentProducts[prodName] = 1
    else:
      shipmentProducts[prodName] += 1
# if any of data_1's shipmentIDs were not already seen in data_2
if error:
  print(error)
  exit(-1)

### FORMAT SHIPMENT DATA ###

# create new sequential index
newID = 0
# need (shipment_id, product_id, quantity, origin, destination)
shipmentTuples = []
for shipmentID in shipmentDict:
  (shipmentOrig, shipmentDest, shipmentProds) = shipmentDict[shipmentID]
  # insert tuple for each product in the shipment
  for prodName in shipmentProds:
    prodID = productDict[prodName]
    quantity = shipmentProds[prodName]
    shipmentTuples.append((newID, prodID, quantity, shipmentOrig, shipmentDest))
    newID += 1
  
### INSERT SHIPMENT DATA ###

# connect to DB
dbConnection = sqlite3.connect(dbName)
# create cursor to execute queries
dbCursor = dbConnection.cursor()
# insert shipmentTuples into shipment table
dbCursor.executemany("INSERT INTO shipment VALUES(?, ?, ?, ?, ?)", shipmentTuples)
# commit transaction
dbConnection.commit()
# close connection
dbConnection.close()