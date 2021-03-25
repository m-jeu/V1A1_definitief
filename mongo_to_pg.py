import PostgresDAO
import MongodbDAO


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


def retrieve_from_dict_depths_recursively(input: dict, keys: list):
    """Function to return value associated with certain key in dictionairy.
    Also able to recursively search any depth of dictionairiy-in-dictionairy construction.

    Will catch KeyError if given a key that doesn't exist, and return None.
    Will also return None if given anything other then an iterable.

    Args:
        input: the dictionairy to access.
        keys: list containing keys, starting from the top level dict going down. One key is also valid.

    Returns:
        Retrieved value if every key in keys was valid.
        None if not.
    """
    if type(input) != dict:
        return None
    if len(keys) == 1:
        return retrieve_from_dict(input, keys[0])
    try:
        return retrieve_from_dict_depths_recursively(input[keys[0]], keys[1:])
    except KeyError:
        return None


def construct_insert_query(table_name: str, var_names: list[str]) -> str:
    """Constructs an SQL insert query, formatted to use %s in place of actual values to allow PsycoPG2 to properly format them.

    Args:
        table_name: the name of the table to insert to insert into.
        var_names: a list containing all the attributes to insert.

    Returns:
        A properly formatted SQL query as string.
    """
    q = f"INSERT INTO {table_name} ("
    q += var_names[0]
    for var in var_names[1:]:
        q += ", " + var
    q += f") VALUES (%s{(len(var_names) - 1) * ', %s'});"
    return q


def simple_mongo_to_sql(mongo_collection_name: str, #TODO: write docstring when computer is not dying
                        postgres_db: PostgresDAO.PostgreSQLdb,
                        postgres_table_name: str,
                        mongo_attribute_list: list[str or list[str]],
                        postgres_attribute_list: list[str],
                        unpack_method_dict: dict = {"shouldnothappen": "shouldnothappen"},
                        reject_if_null_amount: int = 0):
    """A function to do a 'simple', one to one conversion of certain attributes of a MongoDB collection to a PostgreSQL table.

    Args:
        mongo_collection_name: The name of the MongoDB collection to retrieve products from.
        postgres_db: the postgres DB object (from PostgresDAO.py) to insert into.
        postgres_table_name: the postgres_table_name to insert into.
        mongo_attribute_list:
            a list that's allowed to contain either a string, or a list containing strings, for every attribute that has to be retrieved from mongoDB.
            -the key as string if the attribute is directly stored under a single key in the item in MongoDB.
                For instance: "name" if the name is stored under the key name.
            -list containing keys as strings if the attribute is stored within an object (or several, stored within eachother) in MongoDB.
                For instance: ["price", "selling_price"] if the selling price is stored under "selling_price" within an object stored under "price".
        postgres_attribute_list:
            the list of attribute names in PostGreSQL, in the same order as the corresponding attributes in mongo_attribute_list.
            it is recommended to put any primary keys/not null constrained attributes first.
        unpack_method_dict:
            a dictionairy with keys that represent indexes in postgres_attribute_list.
            the values are methods to unpack certain MongoDB attributes in case they need further unpacking.
            for instance, bu_id in the sessions collection is always stored in a length = 1 array.
            a simple function to help unpack this should be passed with unpack_method_dict.
        reject_if_null_amount:
            an integer that represents the first n attributes in both of the attribute lists.
            any entries that have the value null within these attributes will not be entered into PostGreSQL.
        """
    collection = MongodbDAO.getDocuments(mongo_collection_name)
    data_list = []
    for item in collection:
        value_list = []
        for i in range(0, len(mongo_attribute_list)):
            key = mongo_attribute_list[i]
            unpack_method = retrieve_from_dict(dict, i)
            if type(key) == list:
                value = retrieve_from_dict_depths_recursively(item, key)
            else:
                value = retrieve_from_dict(item, key)
            if unpack_method != None:
                value = unpack_method(value)
            if not(isinstance(value, str) or isinstance(value, int) or isinstance(value, float) or value is None):#because pymongo keeps giving us wonky datatypes.
                value = str(value)
            value_list.append(value)
        if not (None in value_list[0:reject_if_null_amount]):
            data_list.append(tuple(value_list))
    q = construct_insert_query(postgres_table_name, postgres_attribute_list)
    postgres_db.many_update_queries(q, data_list)


def fill_profiles_and_bu(pg: PostgresDAO.PostgreSQLdb):
    """Function to specifically fill the Profiles and Bu tables in PostGreSQL from the MongoDB collection profiles.

    Not very modular, still needs improvement.

    Args:
        pg: the PostgresDAO db to fill."""
    collection = MongodbDAO.getDocuments("profiles")
    profile_dataset = []
    buid_dataset = []

    known_buid = set()

    for profile in collection:
        id = str(retrieve_from_dict(profile, "_id"))
        profile_dataset.append((id,))
        buids = retrieve_from_dict(profile, "buids")
        if buids != None:
            for buid in buids:
                buid = str(buid)
                if not (buid in known_buid):
                    known_buid.add(buid)
                    buid_dataset.append((buid, id))
    profile_q = construct_insert_query("Profiles", ["profile_id"])
    buid_q = construct_insert_query("Bu", ["bu_id", "profile_id"])
    pg.many_update_queries(profile_q, profile_dataset)
    pg.many_update_queries(buid_q, buid_dataset)


#TODO: Remove dumb var names, replace with proper function calls.
#refresh DB
PostgresDAO.db.regenerate_db("DDL1.txt")
print("DB CLEANED")


fill_profiles_and_bu(PostgresDAO.db)

#port products

# a = mongo collection name
a = "products"

# b = postgres data base
b = PostgresDAO.db

# c = postgres table name
c = "Products"

# d = mongo attribute list
d = ["_id", "name", ["price", "selling_price"]]

# e = postgres attribute list
e = ["product_id", "product_name", "selling_price"]

# f = unpack_method_dict

# g = reject if null amount
g = 2

simple_mongo_to_sql(a, b, c, d, e, reject_if_null_amount=g)


print("PRODUCTS PORTED")
#port session

def session_buid_unpacker(array):
    """Returns first object in a list.
    Will write nicer function later"""
    return array[0]

# a = mongo collection name
a = "sessions"

# b = postgres data base
b = PostgresDAO.db

# c = postgres table name
c = "Sessions"

# d = mongo attribute list
d = ["_id", "segment", "buid"]

# e = postgres attribute list
e = ["session_id", "segment", "bu_id"]

# f = unpack_method_dict
f = {2: session_buid_unpacker}

# g = reject if null amount
g = 1

simple_mongo_to_sql(a, b, c, d, e, f)
print("SESSIONS PORTED")
