import query_functions
from .. import PostgresDAO

def fill_simple_recommendation(db):
    """Function to make simple recommendation based on the most popular products"""
    # top 4 most popular products
    top_popular_prododucts = query_functions.most_popular_products_query(db)
    #

fill_simple_recommendation(PostgresDAO.db)