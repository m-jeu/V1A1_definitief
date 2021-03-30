from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.database import MongodbDAO
# test


def unpack(input, keys: list):
    """Unpack (several layers) of either dictionairies, tuples or lists (recursively).
    According to a set of keys or indexes.

    Args:
        input: the list, dict or tuple to unpack.
        keys:
            a list containing the indexes or keys to unpack, starting at the top layer, going down.
            a dict-containing-list-containing-dict structure could have a keys parameter like:
                ["Users", 5, "Adress"]

    Returns:
        The unpacked value if all keys/indexes were valid.
        None if not.
    """
    if isinstance(input, dict):
        if len(keys) == 1:
            return input.get(keys[0])
        else:
            return unpack(input.get(keys[0]), keys[1:])
    elif isinstance(input, list) or isinstance(input, tuple):
        try:
            value = input[keys[0]]
        except IndexError:
            return None
        if len(keys) != 1:
            value = unpack(value, keys[1:])
        return value
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


def string_or_none(data) -> str or None:
    """Convert a variable to a string, if it's not None.

    Args:
        data: the variable to convert to a string.

    Returns:
        the converted string, or None if data was already None."""
    if not data is None:
        return str(data)
    return None


def simple_mongo_to_sql(mongo_collection_name: str,
                        postgres_db: PostgresDAO.PostgreSQLdb,
                        postgres_table_name: str,
                        mongo_attribute_list: list[str or list[str]],
                        postgres_attribute_list: list[str],
                        unpack_method_dict: dict = {"shouldnothappen": "shouldnothappen"},
                        reject_if_null_amount: int = 0,
                        remember_in_existance: int = None):
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
        remember_unique_attributes:
            the index of an attribute that entries in existance have to be stored in python for.
            if True, returns set of all entries of a certain attribute in existance.

    Returns:
        if remember_unique_attributes is True, returns set of all entries of a certain attribute in existance.
        returns None otherwise
        """
    collection = MongodbDAO.getDocuments(mongo_collection_name)
    data_list = []
    if not remember_in_existance is None: #TODO: think of better var names
        remember_things_in_existance_set = set()
    for item in collection:
        value_list = []
        for i in range(0, len(mongo_attribute_list)):
            key = mongo_attribute_list[i]
            unpack_method = unpack_method_dict.get(i)
            if type(key) == list:
                value = unpack(item, key)
            else:
                value = item.get(key)
            if unpack_method != None:
                value = unpack_method(value)
            if not(isinstance(value, str) or isinstance(value, int) or isinstance(value, float) or value is None):#because pymongo keeps giving us wonky datatypes. TODO: Consider always using str() without checking for type
                value = str(value)
            if remember_in_existance == i:
                remember_things_in_existance_set.add(value)
            value_list.append(value)
        if not (None in value_list[0:reject_if_null_amount]):
            data_list.append(tuple(value_list))
    q = construct_insert_query(postgres_table_name, postgres_attribute_list)
    postgres_db.many_update_queries(q, data_list, fast_execution=True)
    if not remember_in_existance is None:
        return remember_things_in_existance_set


def fill_sessions_profiles_bu(db: PostgresDAO.PostgreSQLdb, valid_product_ids: set):
    """Fill the session, profile and bu tables in the PostgreSQL db using the profiles and session collections in MongoDB.

    Not very modular, but considering the specific nature of (almost) every PostgreSQL table, a specific function is
    probably the most pragmatic way to go about it.



    Args:
        db: the PostgresDAO.postgreSQLdb object to fill."""
    profile_collection = MongodbDAO.getDocuments("profiles")
    session_collection = MongodbDAO.getDocuments("sessions")

    session_dataset = []
    profile_dataset = []
    buid_dataset = []
    ordered_products_dataset = []
    ordered_products_dict = {} #keeps track of all the orders with (session_id, product_id) as key and quantity as value
    profile_set = set() #keeps track of all the profile_ids that exist in the profiles collection
    buid_dict = {} #keeps track of all the buids that exist in the session collection dataset (as keys) and what profile they're associated with (if any) as values.

    for session in session_collection:
        #get session information and add to session dataset
        session_id = session.get("_id")
        if session_id is None:
            continue
        session_segment = session.get("segment")
        session_buid = unpack(session, ["buid", 0])

        if isinstance(session_buid, list):
            session_buid = unpack(session_buid, [0])

        session_id = string_or_none(session_id)
        session_segment = string_or_none(session_segment)
        session_buid = string_or_none(session_buid)

        session_tuple = (session_id, session_segment, session_buid)
        session_dataset.append(session_tuple)

        #add session_buid to buid_dict
        buid_dict[session_buid] = None

        #add products that have been ordered to ordered_products_dict
        session_order = unpack(session, ["order", "products"])
        if session_order != None:
            for product in session_order:
                product_id = string_or_none(product.get("id"))
                if product_id in valid_product_ids:
                    if (session_id, product_id) in ordered_products_dict:
                        ordered_products_dict[(session_id, product_id)] += 1
                    else:
                        ordered_products_dict[(session_id, product_id)] = 1

    for profile in profile_collection:
        #get profile information and add to profile_set
        profile_id = string_or_none(profile.get("_id"))
        profile_buids = profile.get("buids")
        profile_set.add(profile_id)

        #assign profile_id associated buid in buid_Dict
        if isinstance(profile_buids, list):
            for profile_buid in profile_buids:
                profile_buid = string_or_none(profile_buid)
                if profile_buid in buid_dict:
                    buid_dict[profile_buid] = profile_id

    #fill buid_dataset from buid_dict
    for buid, profile in buid_dict.items():
        buid_dataset.append((buid, profile))

    #fill profile_dataset from profile_set
    profile_dataset = [(x,) for x in profile_set]

    #fill ordered_products_dataset from ordered_products_dict
    for k, v in ordered_products_dict.items():
        ordered_products_dataset += (k[0], k[1], v)

    #construct insert queries for PostgreSQL insertions
    profile_query = construct_insert_query("Profiles", ["profile_id"])
    bu_query = construct_insert_query("Bu", ["bu_id", "profile_id"])
    session_query = construct_insert_query("Sessions", ["session_id", "segment", "bu_id"])
    ordered_products_query = construct_insert_query("Ordered_products", ["session_id", "product_id", "quantity"])

    #insert into PostgreSQL
    db.many_update_queries(profile_query, profile_dataset, fast_execution=True)
    db.many_update_queries(bu_query, buid_dataset, fast_execution=True)
    db.many_update_queries(session_query, session_dataset, fast_execution=True)
    db.many_update_queries(ordered_products_query, ordered_products_dataset, fast_execution=True)



### Actual function calls to fill the database
if __name__ == "__main__":
    print("--START MONGO-TO-PG--", end="\n\n\n")

    #Regenerate DB
    print("Regenerating the PostgreSQL database.")
    PostgresDAO.db.regenerate_db("DDL1.txt")
    print("PostgreSQL database has been regenerated!", end="\n\n")

    #Fill the products table in PostgreSQL TODO: Add all the missing attributes
    print("Filling the Products table.")
    products_in_existance = simple_mongo_to_sql("products",
                            PostgresDAO.db,
                            "Products",
                            ["_id", "name", ["price", "selling_price"], "category", "sub_category", "sub_sub_category"],
                            ["product_id", "product_name", "selling_price", "category", "sub_category", "sub_sub_category"],
                            reject_if_null_amount=2,
                            remember_in_existance=0)
    print("The Products table has been filled!", end="\n\n")

    #Fill the Profiles, Bu and Sessions tables in PostgreSQL
    print("Filling the Profiles, Bu and Sessions tables.")
    fill_sessions_profiles_bu(PostgresDAO.db, products_in_existance)
    print("The Profiles, Bu and Sessions tables have been filled!", end="\n\n")
