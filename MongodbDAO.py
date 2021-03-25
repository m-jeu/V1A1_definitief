import pymongo

def getMongoDB(mongoConnectString = "mongodb://localhost:27017/", databaseName = 'huwebshop'):
    """Functie om te connecten met een mongoDB
    Args:
        mongoConnectString: connect string standaard connection op localhost:27017
        databaseName: naam van de database standaard huwebshop
    Returns connectie met mongoDB"""
    myclient = pymongo.MongoClient(mongoConnectString)
    return myclient[databaseName]

def getCollection(collectionName):
    """Functie om een collectie uit de mongoDB op te halen
    Args:
        collectionName: Naam van de op te halen collectie
    Returns de collectie als object"""
    return getMongoDB().get_collection(collectionName)

def getDocuments(collectionName, filter = {}):
    """Functie om een collectie uit de mongoDB op te halen met een filter op de elementen
    Args:
        collectionName: Naam van de op te halen collectie
        filter: De key en value waarop gefilterd moet worden bijv: "{'category': 'Gezond & verzorging'}
    Returns de gefilterde collectie als object
        """
    return getCollection(collectionName).find(filter)
###EXPIRIMENTAL

def retrieve_from_dict(dict: dict, key):
    """Tries to retrieve a value from a dictionairy with a certain key, and catches the KeyError if it doesn't exist.

    Args:
        dict: the dictionairy to retrieve the value from.
        key: the key associated with the desired data.

    Returns:
        if the key exists in the dictionairy, returns the value associated with the key.
        if it doesn't exist, returns None"""
    try:
        return dict[key]
    except KeyError:
        return None
