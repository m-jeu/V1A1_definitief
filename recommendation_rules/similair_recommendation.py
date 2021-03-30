from pandas import DataFrame
import pandas.io.sql as psql
from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import query_functions

def propositional_logic_recommendation(db, table, pandas_query, query_attributes):
    """Function to make a table for a and_or_recommendation (similair) and fill them based on a given query
    ex query: product_id != "%s" and sub_sub_category == "%s" and (selling_price > %s*0.80 and selling_price < %s *1.20)
    will fill the table with products that are in the same sub_sub_category and the price is between 80 & 120 % as current product
    args:
        db: is a PostgresDAO.database object used to connect with the psql database
        table: tablename where the recommendation will be stored. ~~ recommendation name
        pandas_query: the filter query including %s formats where the values of the current product must be formatted
        query_attributes: the attributes needed to format in the pandas query
    ex pandasquery: (product_id != "%s" and sub_sub_category == "%s" and (selling_price > %s*0.80 and selling_price < %s *1.20)
    ex query_ attributes: 'product_id, sub_sub_category, selling_price, selling_price'
    """
    # create recommendation table in psql
    query_functions.create_rec_table_query(db, table, "prod_ids VARCHAR,")
    # weird but working way to connect with psql psql.read_sql_query requires connection
    db._connect()
    # select all from products # todo implement way to FROM profiles with param
    all_products = DataFrame(psql.read_sql_query("SELECT * FROM products", db.connection))
    # create pandas df
    df = DataFrame(all_products)
    # because we queried to psql with pandas not with psycopg2 we can set the column names from the keys, psycopy2 doesn't return keys.
    df.columns = all_products.keys()
    # list where all recommendations will be stored so they can be inserted into psql at once.
    all_recommendations = []
    # for all indexes in length of df (will be used to itterate over each product)
    for i in range(0, len(df)):
        # attribute values of the current product (only the attributes passed as param)
        # ex: ('8532', 'Deodorant', 179, 179)
        # note we used price twice so we can do price > %s * 0.80 and price < * 1.20
        attribute_values = tuple([df[column][i] for column in query_attributes])
        # query over the df find products where a certain filter(filter defined in function call) matches the current product
        # recs = all recommendations in a df
        recs = df.query(pandas_query%attribute_values)
        # upload list is list of the first 4 product id's in the available recs [column_names.split(', ')[0]]
        # because of iloc upto the first 4 elements will be taken no indexing error! :)
        recommendations = list(recs.iloc[:4]['product_id'])
        # and insert the current id at recommendations[0] this will be the PK! this is the id the recommendation is made for
        recommendations.insert(0, df['product_id'][i])
        # make a tuple for insert
        recommendations = tuple(recommendations)
        # insert into all_recommendations
        all_recommendations.append((recommendations, ))
    # fill the recommendation table with all the recommendations
    db.many_update_queries(f"INSERT INTO {table} VALUES %s", all_recommendations)

propositional_logic_recommendation(PostgresDAO.db,
                      'and_or_recommendation',
                     """(product_id != "%s" and sub_sub_category == "%s" and (selling_price > %s*0.80 and selling_price < %s *1.20))""",
                      ['product_id', 'sub_sub_category', 'selling_price', 'selling_price'])



