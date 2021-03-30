# also executes query
def create_rec_table_query(db, table_name, attributes_datatypes):
    """Function to construct and execute a query for table creation where the product recommendations will be stored
    args:
        db: PostreSQL object where to create the table. db is a object from PostgresDAO
        table_name: the name of the table that will be created. the table name will be the name of the recommendation rule
        attributes_datatypes:   the attributes which the recommendation engine will use to fill the tables and their data type ex: 'category VARCHAR, targetaudience VARCHAR,'
                                all attributes and datatypes must be stored in a single string"""
    table_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name}
    (
    {attributes_datatypes}
    pro1 VARCHAR,
    pro2 VARCHAR,
    pro3 VARCHAR,
    pro4 VARCHAR,
    FOREIGN KEY(pro1) REFERENCES products(product_id),
    FOREIGN KEY(pro2) REFERENCES products(product_id),
    FOREIGN KEY(pro3) REFERENCES products(product_id),
    FOREIGN KEY(pro4) REFERENCES products(product_id)
    );"""
    db.query(table_query, commit_changes=True)
# also executes query
def most_popular_products_query(db):
    return db.query("SELECT product_id from ordered_products group by product_id order by count(product_id) DESC limit 4", expect_return=True)
