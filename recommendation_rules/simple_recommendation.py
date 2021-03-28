from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import query_functions

def fill_simple_recommendation(db):
    """Function to make simple recommendation based on the most popular products"""
    # create table
    query_functions.create_rec_table_query(db, 'simple_recommendation', '')
    # top 4 most popular products
    top_popular_products = query_functions.most_popular_products_query(db)
    recommendations = tuple([top_popular_products[i][0] for i in range(len(top_popular_products))])
    db.query(
        f"INSERT INTO simple_recommendation VALUES %s", (recommendations, ), commit_changes=True)

fill_simple_recommendation(PostgresDAO.db)


