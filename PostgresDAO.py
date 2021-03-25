import psycopg2
import random
if __name__ == "__main__": #for formatieve opdracht 2c
    import MongodbDAO
    import pymongo




#Vul hier je eigen PostgreSQL credentials in:
host = "localhost"
database = "rcmd"
user = "postgres"
password = "DataBAE117"
port = "5432"
#try not to push them to github



class PostgreSQLdb:
    """A PostgreSQL (relational) database.

    Attributes:
        host: the hostname the database is hosted at (presumably localhost in this usecase).
        database: the name of the database (rcmd for all the group members).
        user: the user the database belongs to according to pgadmin (presumably postgres).
        password: the database password.
        port: the port the database is hosted at (presumably 5432 if DB is hosted on windows machine).
        connection: a psycopg2.extensions.cursor object that represents a connection to the database.
        cursor: a psycopg2.extensions.connection that represents a cursor in the database."""
    def __init__(self, host: str, database: str, user: str, password: str, port: str):
        """Initialize class instance.

        Args:
            host: the hostname the database is hosted at (presumably localhost in this usecase).
            database: the name of the database (rcmd for all the group members).
            user: the user the database belongs to according to pgadmin (presumably postgres).
            password: the database password.
            port: the port the database is hosted at.
            """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None

    def _connect(self):
        """Connects to the database.
        Assings a connection object to self.connection."""
        self.connection = psycopg2.connect(
            host = self.host,
            database = self.database,
            user = self.user,
            password = self.password,
            port = self.port
        )

    def _summon_cursor(self):
        """Creates a cursor within the database.
        Assigns a cursor object to self.cursor."""
        self.cursor = self.connection.cursor()

    def _close_connection(self):
        """Closes the connection with the database.
        Sets self.connection to none."""
        self.connection.close()
        self.connection = None

    def _close_cursor(self):
        """Closes the cursor within the database.
        Sets self.cursor to none."""
        self.cursor.close()
        self.cursor = None

    def _bare_query(self, query: str, parameters: tuple = None):
        """Executes an SQL query in the database.
        Able to sanatize inputs with the parameters parameter.

        Args:
            query:
                The SQL query to execute.
                May contain '%s' in place of parameters to let psycopg2 automatically format them.
                The values to replace the %s's with can be passed with the parameters parameter.
            parameters: tuple containing parameters to replace the %s's in the query with."""
        if parameters == None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, parameters)

    def _fetch_query_result(self) -> list[tuple]:
        """Get the results from the most recent query.

        Returns:
            list containing a tuple for every row in the query result."""
        return self.cursor.fetchall()

    def _commit_changes(self):
        """Commits any changes made in queries."""
        self.connection.commit()

    def query(self, query: str, parameters: tuple = None, expect_return: bool = False, commit_changes: bool = False) -> list[tuple] or None:
        """Opens a connection to the DB, opens a cursor, executes a query, and closes everything again.
        Also retrieves query result or commits changes if relevant.

        Args:
            query:
                The SQL query to execute.
                May contain '%s' in place of parameters to let psycopg2 automatically format them.
                The values to replace the %s's with can be passed with the parameters parameter.
            parameters: tuple containing parameters to replace the %s's in the query with.
            expect_return: bool for wether the query should expect a return.
            commit_changes: bool for wether changes were made to the DB that need to be committed.

        Returns:
            if expect_return:
                list containing a tuple for every row in the query result.
            else:
                None"""
        self._connect()
        self._summon_cursor()
        self._bare_query(query, parameters)
        output = None
        if expect_return:
            output = self._fetch_query_result()
        if commit_changes:
            self._commit_changes()
        self._close_cursor()
        self._close_connection()
        return output

    def many_update_queries(self, query: str, data_list: list[tuple]):
        """Execute a single query that updates the DB with many different values without constantly opening/closing cursors/connections.

        Args:
            query:
                The SQL query to execute with every dataset in data_list.
                Must contain '%s' in place of parameters to let psycopg2 automatically format them.
                The values to replace the %s's with must be passed with the parameters parameter.
            data_list:
                list containing a tuple for every query that has to be excecuted."""
        self._connect()
        self._summon_cursor()
        self.cursor.executemany(query, data_list)
        self._commit_changes()
        self._close_cursor()
        self._close_connection()


    def regenerate_db(self, ddl_source: str):
        """Empties all knows tables in the DB, and reconstructs everything according to the DDL(SQL) file provided.

        Args:
            ddl_source: the file path/name of the ddl script."""
        with open(ddl_source, "r") as file:
            file_object = file.read().replace('\n', '')
        query_list = file_object.split(";")
        for query in query_list:
            if query != "":
                self.query(query + ";", commit_changes=True)



#functie voor opdracht 1 voor summatieve opdracht 2c:
def add_items_to_database(db: PostgreSQLdb):
    """makes a connection to the PostgreSQL database.
    calls the regenerate function, that drops the existing tables from the database, and creates empty new ones.
    loops trough items in range(amount) from the MongoDB database and adds them to the PostgreSQL database.

    parameters:
        db: the postgres database to fill."""

    db.regenerate_db("DDL1.txt")

    products = MongodbDAO.getDocuments("products")
    for i in range(20):
        product_name = products[i]["name"]
        product_id = products[i]["_id"]
        product_price = products[i]["price"]["selling_price"]
        db.query("INSERT INTO Products (product_id, product_name, selling_price) VALUES (%s, %s, %s);",
                 (product_id, product_name, product_price), commit_changes=True)

# functie voor opdracht 2 voor summatieve opdracht 2c:
def alle_product_ids(db: PostgreSQLdb):
    """Functie die alle product ids returned in een lijst uit de relationele database

    parameters:
        db: de database om ids uit te halen."""
    product_ids = []
    products = db.query("SELECT * FROM products", expect_return=True)
    for product in products:
        product_ids.append(product[0])
    return product_ids

def product_id_lijst_input():
    """Functie voor het verkrijgen van een product id lijst doormiddel van user input.
    Een lijst van alle opgegeven product ids wordt returned"""
    product_ids = []
    while True:
        productid = input('input een product id, input ok als je je klaar bent met ids invoeren')
        # als input ok is stop dan met id's vragen
        if productid == 'ok':
            break
        # append id aan product ids lijst
        product_ids.append(productid)
    return product_ids

def gemiddelde_prijs(id_lijst, db: PostgreSQLdb):
    """Functie die de gemiddelde prijs van een gegeven lijst product ids berekent en deze returned (in eurocent)
    args:
        id_lijst: lijst van product ids waarvan het gemiddelde berekent wordt
        db: de database om te doorzoeken"""
    prijs_lijst = []
    for id in id_lijst:
        product = db.query(f"SELECT selling_price FROM products WHERE product_id = '{id}'", expect_return=True)
        # index 0 is tuple met alle informatie, daarvan index 2 is de prijs
        prijs_lijst.append(product[0][0])
    gemiddeld = sum(prijs_lijst) / len(prijs_lijst)
    return round(gemiddeld)


#functie voor opdracht 3 summatieve opdracht 2c:
def max_abs_price(db: PostgreSQLdb) -> tuple[str]:
    """Retrieves all products in the database.
    Selects a random product from that database.
    Searches through all products for the product with biggest price difference compared to that one.

    Args:
        db: the database to retrieve products from.

    Returns:
        tuple containing:
            0: product_id of the product with the biggest price difference.
            1: product_id of the randomly selected product.
    """
    sql_query = "SELECT product_id, selling_price FROM Products;"
    products = db.query(sql_query, expect_return=True)
    random_product = random.choice(products)
    biggest_dif_product, biggest_dif = None, -1
    for product in products:
        dif = abs(random_product[1] - product[1])
        if dif > biggest_dif:
            biggest_dif_product, biggest_dif = product, dif
    return biggest_dif_product[0], random_product[0]


db = PostgreSQLdb(host, database, user, password, port)



"""
#function calls for summatieve opdracht 2c:
print("1:", end="\n\n")
print(f"Adding items to PostGreSQL...")
add_items_to_database(db)
print("Done!")

print("2:", end="\n\n")
print(f"De gemiddelde prijs van alle producten in PostGreSQL is {gemiddelde_prijs(alle_product_ids(db), db)}.")
print("Vul ID's in van producten waarvan je de gemiddelde prijs wilt weten. Vul 'ok' in als je klaar bent.")
print(gemiddelde_prijs(product_id_lijst_input(), db))

print("3:", end="\n\n")
map = max_abs_price(db)
print(f"ID van gekozen willikeurig product: {map[1]}")
print(f"ID van grootste absolute prijsverschil met dat product in PostGreSQL: {map[0]}")
"""