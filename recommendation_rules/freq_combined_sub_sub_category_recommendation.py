import pandas.io.sql as psql
from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import propositional_logic_recommendation, query_functions

def sub_sub_recommendations(db, table_name, quantity):
    """
    Function to create recommendations for every product based on with which sub_sub_category they have been combined most often in historical orders
    If there is not enough recommendations with this logic the remaining recommendations needed will be gained from the function most_similair in
    propositional_logic_recommendation.py. These recommendations will be inserted into the table sub_sub_recommendations
    Args:
        db: Postgres.DAO database object
        table_name: the name of the table that will be created."""
    # all product_id's and their most frequently combined sub_sub_category
    freq_combined = psql.read_sql_query(f"SELECT product_id , sub_sub_category FROM freq_combined;",db._connect())
    # all products
    products = psql.read_sql_query(f"SELECT * FROM products;", db._connect())
    # create new table for the recommendation
    query_functions.create_rec_table_query(db, table_name, "prod_id VARCHAR, ")
    # final upload list
    recommendation_list = []
    # for each index in len(products)
    for i in range(0, len(products)):
        # new df from products with the same sub_sub_category as the current index in freq_combined
        product_df = products.query("""sub_sub_category == "%s" """%freq_combined.iloc[i]["sub_sub_category"])
        # get recommendations from that df
        recs = product_df["product_id"].sample(n=min(len(product_df), quantity)).values.tolist()
        original_id = freq_combined.iloc[i]["product_id"]
        # list of the current product_id in freq combined and all recs
        product_id_list =[original_id, * recs]
        # if not enough recs
        if len(recs) < quantity:
            # get extra recs with the function most_similar
            extra_recs = propositional_logic_recommendation.most_similar(recs, quantity, products, products.iloc[i], 0.15)
            product_id_list += extra_recs
        recommendation_list.append((tuple(product_id_list), ))
    PostgresDAO.db.many_update_queries(f"INSERT INTO {table_name} VALUES %s ", recommendation_list)

if __name__ == "__main__":
    sub_sub_recommendations(PostgresDAO.db, "freq_combined_sub_sub_category", 4)