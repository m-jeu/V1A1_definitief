import pymongo


def getMongoDB(mongoConnectString = "mongodb://localhost:27017/", databaseName = 'huwebshop'): #TODO: Make more modular
    """Function to connect to mongoDB
    Args:
        mongoConnectString: connection string for the standard connection on the localhost:27017
        databaseName: name of the database, the standard being huwebshop
    Returns connection to mongoDB"""
    myclient = pymongo.MongoClient(mongoConnectString)
    return myclient


def getCollection(collectionName, database_name = "huwebshop"):
    """function to get a collection from mongoDB
    Args:
        collectionName: name of the collection to be collected
    Returns the collections as an object"""
    mongo_client = getMongoDB()
    result = mongo_client[database_name].get_collection(collectionName)
    mongo_client.close()
    return result


def getDocuments(collectionName, filter = {}):
    """Function to get a collection from mongoDB with a filter on elements
    Args:
        collectionName: name of the collection to be collected from
        filter: key and value on what to filter: ex: "{'category': 'Gezond & verzorging'}"
    Returns the filterd collection as an object"""
    return getCollection(collectionName).find(filter)