import pandas.io.sql as psql
from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import query_functions
import pandas
pandas.set_option('display.max_columns', 10)


def AND_OR(db, table, boolean_query, price_diff, quantity):
    query_functions.create_rec_table_query(db, table, "prod_id VARCHAR,")
    # import all products
    df = psql.read_sql_query("SELECT * FROM products limit 10", db._connect())
    db._close_connection()
    all_recommendations = []
    for i in range(0, len(df)):
        df_boolean = (df.iloc[i] == df).astype(int)
        df_boolean['selling_price'] = df['selling_price'].between(df.iloc[0]['selling_price']*(1-price_diff), df.iloc[0]['selling_price']*(1+price_diff)).astype(int)
        df_boolean['product_id'] = df['product_id']
        possible_recommendations = df_boolean.query(boolean_query)
        if len(possible_recommendations) < quantity:
            sample_n = len(possible_recommendations)
            recommendations = list(possible_recommendations.sample(n=sample_n)['product_id'])
            df_boolean = df_boolean.query("""product_id != %s """ % recommendations)
            boolean_score = pandas.concat([df_boolean['product_id'], df_boolean.sum(axis=1)], axis=1)
            boolean_score.columns = ['product_id', 'bool_count']
            boolean_score = boolean_score.sort_values(by=['bool_count'])
            sample_n = quantity - len(possible_recommendations)
            recommendations = recommendations + boolean_score[-sample_n:]['product_id'].values.tolist()
        else:
            sample_n = quantity
            recommendations = possible_recommendations.sample(n=sample_n)['product_id']
        #recommendations.insert(0, df['product_id'][i])

        recommendations.insert(0, df['product_id'][i])
        # make a tuple for insert

        recommendations = tuple(recommendations)
        # insert into all_recommendations
        all_recommendations.append((recommendations, ))
    # fill the recommendation table with all the recommendations
    db.many_update_queries(f"INSERT INTO {table} VALUES %s", all_recommendations)

AND_OR(PostgresDAO.db, "testtable","product_id == False and selling_price == True and sub_sub_category == True", 0.15, 4)