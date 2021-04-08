from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import query_functions
import pandas.io.sql as psql
import pandas
from datetime import datetime
import itertools

def frequently_combined(db):
    """"Function that creates a new table where the most frequent combination of a product_id and sub_sub_category based on historical orders is saved
        For each existing product in the psql database it will search for the most frequently combined sub_sub_category. If there is no historical data available
         the sub_sub_category of the current product will be chosen
    args:
        db: Postgres.DAO database object"""
    # df with all sessions.
    # Each session grouped by it's id and all product_id's ordered by that session are grouped into 1 array
    # ex :
    #   session 1 order: [1, 2, 3]
    #   session 2 order: [5, 2, 1, 9] etc.
    sessions = psql.read_sql_query("""with orders AS(select session_id, array_agg(product_id) as all_ids
                                        from ordered_products group by session_id) select * from orders""", db._connect())
    db._close_connection()
    # create a new table where the combinations will be inserted
    query_functions.create_small_table(db, 'freq_combined', """product_id VARCHAR,
    sub_sub_category VARCHAR""")
    # df with all products
    products = psql.read_sql_query("""select * from products""", db._connect())
    db._close_connection()

    # eventual upload list that will be inserted into the combination table.
    upload_list = []
    # for all indexes in length of the products df (will be used to iterate over each product)
    for i in range(0, len(products)):
        # current product_id in iteration
        cur_prod_id = products.iloc[i]['product_id']
        # find all sessions where the current product_id is present by checking if cur_prod_id is a subset of an array/set
        # in the Pandas Dataframe
        session_list = sessions[sessions['all_ids'].map(set([cur_prod_id]).issubset)]
        # transform those values to a list, this list contains all product_id's combined with the cur_prod_id
        product_id_list = session_list['all_ids'].values.tolist()
        # currently product_id_list contains lists for each session, we need 1 list with all product id's, not seperated by session.
        # itertools.chain is used to unpack the list of lists containing product_ids to a list of product_ids
        product_id_df = pandas.DataFrame(columns=['prod_id'], data=list(itertools.chain(*product_id_list)))
        # make a list out of all the product_id's without the cur_prod_id because it needs to be ignored for finding most combined sub_sub_category
        product_id_df = product_id_df[product_id_df['prod_id'] != cur_prod_id].values.tolist()

        # if the product has never been bought or only bought by itself. we store cur_prod_id's sub_sub_category as most_combined
        if len(product_id_df) <= 0:
            most_combined = products.query("""product_id == "%s" """%cur_prod_id)['sub_sub_category'].values[0]
        else:
            x = list(itertools.chain(*product_id_df))
            all_categories_count = products.query("""product_id == %s """ % x)
            most_combined = all_categories_count['sub_sub_category'].value_counts(dropna=False).idxmax()

        upload_list.append((tuple([cur_prod_id] + [most_combined]), ))
    db.many_update_queries(f"INSERT INTO freq_combined VALUES %s", upload_list)

# temporary code to measure time to run


def start_frequently_combined():
    now = datetime.now()
    now = now.strftime("%H:%M:%S")
    print("start time", now)

    frequently_combined(PostgresDAO.db)

    # temporary code to measure time to run
    now = datetime.now()
    now = now.strftime("%H:%M:%S")
    print("end time =", now)


if __name__ == "__main__":
    start_frequently_combined()
