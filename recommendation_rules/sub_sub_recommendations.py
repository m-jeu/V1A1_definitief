import pandas.io.sql as psql
from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import propositional_logic_recommendation, query_functions


def sub_sub_recommendations(db, table_name):
    """This function creates recommendations based on the most common sub_sub_category for each product.
    If there are less then 4 products to recommend using this recommendation,
    it will create extra recommendations using the most_similar function in the propositional_logic_recommendation file.
    These recommendations will be put in the database under each product.

    Args:
        db: Postgres.DAO database object
        table_name: the name of the table that will be created."""
    freq_combined = psql.read_sql_query(f"SELECT product_id , sub_sub_category FROM {table_name};",db._connect())
    products = psql.read_sql_query(f"SELECT * FROM products;", db._connect())
    query_functions.create_rec_table_query(db, "sub_sub_recommendations", "prod_id VARCHAR, ")
    recommendation_list = []
    for p in range(0, len(products)):
        product_df = products.query("""sub_sub_category == "%s" """%freq_combined.iloc[p]["sub_sub_category"])
        recs = product_df["product_id"].sample(n=min(len(product_df), 4)).values.tolist()
        original_id = freq_combined.iloc[p]["product_id"]
        product_id_list =[original_id, * recs]
        if len(recs) < 4:
            extra_recs = propositional_logic_recommendation.most_similar(recs, 4, products, products.iloc[p], 0.15)
            product_id_list += extra_recs
        recommendation_list.append((tuple(product_id_list), ))
    PostgresDAO.db.many_update_queries("INSERT INTO sub_sub_recommendations VALUES %s ", recommendation_list)


if __name__ == "__main__":
    sub_sub_recommendations(PostgresDAO.db, "freq_combined")
