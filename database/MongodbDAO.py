import pymongo

def getMongoDB(mongoConnectString = "mongodb://localhost:27017/", databaseName = 'huwebshop'): #TODO: Make more modular
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
