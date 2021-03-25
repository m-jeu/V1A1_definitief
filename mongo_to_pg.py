import PostgresDAO
import MongodbDAO
# test


def retrieve_from_list(lst: list, index: int):
    """Try to retrieve an item at a certain index in a list.
    Check if parameter is actually a list.

    If given list is not actually an list, or given index doesn't exist in it, returns None.

    Args:
        """
    if isinstance(lst, list):
        try:
            return lst[index]
        except IndexError:
            return None #could also perhaps be a pass statement
    return None

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
            unpack_method = retrieve_from_dict(unpack_method_dict, i)
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
    postgres_db.many_update_queries(q, data_list, fast_execution=True)


def fill_sessions_profiles_bu(db: PostgresDAO.PostgreSQLdb):
    """Fill the session, profile and bu tables in the PostgreSQL db using the profiles and session collections in MongoDB.

    Args:
        db: the PostgresDAO.postgreSQLdb object to fill."""
    profile_collection = MongodbDAO.getDocuments("profiles")
    session_collection = MongodbDAO.getDocuments("sessions")

    session_dataset = []
    profile_dataset = []
    buid_dataset = []
    profile_set = set()
    buid_dict = {}

    for session in session_collection:
        session_id = str(retrieve_from_dict(session, "_id"))
        session_segment = str(retrieve_from_dict(session, "segment"))
        session_buid = retrieve_from_list(retrieve_from_dict(session, "buid"), 0)
        if isinstance(session_buid, list): #FIXME: Should be handeled by retrieve_from_list() func.
            session_buid = retrieve_from_list(session_buid, 0)

        session_buid = str(session_buid) #FIXME: Find better place to put this

        session_tuple = (session_id, session_segment, session_buid)

        session_dataset.append(session_tuple)
        if not session_buid in buid_dict:#could perhaps remove if-statement and just re-assign None
            buid_dict[session_buid] = None

    for profile in profile_collection:
        profile_id = str(retrieve_from_dict(profile, "_id"))
        profile_buids = retrieve_from_dict(profile, "buids")

        if isinstance(profile_buids, list):
            for profile_buid in profile_buids:
                profile_buid = str(profile_buid)
                if profile_buid in buid_dict:
                    buid_dict[profile_buid] = profile_id

        profile_set.add(profile_id)

    for buid, profile in buid_dict.items():
        buid_dataset.append((buid, profile))

    profile_dataset = [(x,) for x in profile_set]

    profile_query = construct_insert_query("Profiles", ["profile_id"])
    bu_query = construct_insert_query("Bu", ["bu_id", "profile_id"])
    session_query = construct_insert_query("Sessions", ["session_id", "segment", "bu_id"])

    db.many_update_queries(profile_query, profile_dataset, fast_execution=True)
    db.many_update_queries(bu_query, buid_dataset, fast_execution=True)
    db.many_update_queries(session_query, session_dataset, fast_execution=True)



### Actual function calls to fill the database
if __name__ == "__main__":
    print("--START MONGO-TO-PG--", end="\n\n\n")

    #Regenerate DB
    print("Regenerating the PostgreSQL database.")
    PostgresDAO.db.regenerate_db("DDL1.txt")
    print("PostgreSQL database has been regenerated!", end="\n\n")

    #Fill the products table in PostgreSQL TODO: Add all the missing attributes
    print("Filling the Products table.")
    simple_mongo_to_sql("products",
                        PostgresDAO.db,
                        "Products",
                        ["_id", "name", ["price", "selling_price"], "category", "sub_category", "sub_sub_category"],
                        ["product_id", "product_name", "selling_price", "category", "sub_category", "sub_sub_category"],
                        reject_if_null_amount=2)
    print("The Products table has been filled!", end="\n\n")

    #Fill the Profiles, Bu and Sessions tables in PostgreSQL
    print("Filling the Profiles, Bu and Sessions tables.")
    fill_sessions_profiles_bu(PostgresDAO.db)
    print("The Profiles, Bu and Sessions tables have been filled!", end="\n\n")


