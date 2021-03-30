from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import query_functions

def fill_simple_recommendation(db):
    """Function to make a table for a simple recommendation and fill them based on the most popular products
    args:
        db: is a PostgresDAO.database object used to connect with the psql database
    returns nothing"""

    # create table
    query_functions.create_rec_table_query(db, 'simple_recommendation', '')
    # top 4 most popular products
    top_popular_products = query_functions.most_popular_products_query(db)
    # make a tuple of the recommendations
    recommendations = tuple([top_popular_products[i][0] for i in range(len(top_popular_products))])
    # insert into the simple_recommendation table
    db.query(
        f"INSERT INTO simple_recommendation VALUES %s", (recommendations, ), commit_changes=True)

fill_simple_recommendation(PostgresDAO.db)