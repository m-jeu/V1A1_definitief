import pandas.io.sql as psql
from V1A1_definitief.database import PostgresDAO
from V1A1_definitief.recommendation_rules import query_functions
import pandas

# store functions calls/logic for a new recommendation here
recommendation_dict = {
    'sub_sub_category': [
                                   """(product_id != "%s" and sub_sub_category == "%s")""",
                                    ['product_id', 'sub_sub_category']]}

def most_similar(current_recs, quantity, df, current_df, diff):
    """"Function that returns a list of the most similar products based on a boolean score (+1 for matching attribute +0 for not matching)
        This is a back-up recommendation so a list of the current recommendations must be passed so we don't recommend the same products twice
        args:
            current_recs: a list of the current recommendations
            quantity:     the quantity of total(!) required recommendations
            df:           a pandas dataframe of all products including their attributes
            current_df:   a pandas dataframe of the product which a recommendation made for including it's attributes
            diff:         the maximum difference in price for to be 'similar' ex: 0.15 is a maximum price diff of 15%
        returns:
            a list containing only(!) the recommendations made based on the boolean count"""

    df_boolean = (current_df == df).astype(int)
    df_boolean['selling_price'] = df['selling_price'].between(df.iloc[0]['selling_price'] * (1 - diff),
                                                              df.iloc[0]['selling_price'] * (
                                                                      1 + diff)).astype(int)
    df_boolean['product_id'] = df['product_id']
    current_ids = list(current_df['product_id']) + current_recs
    df_boolean = df_boolean.query("""product_id != %s""" % current_ids)
    boolean_score = pandas.concat([df_boolean['product_id'], df_boolean.sum(axis=1)], axis=1)
    boolean_score.columns = ['product_id', 'bool_count']
    boolean_score = boolean_score.sort_values(by=['bool_count'])
    sample_n = quantity - len(current_recs)
    # recommendations = recommendations +
    return boolean_score[-sample_n:]['product_id'].values.tolist()

def propositional_logic_recommendation(db, table, pandas_query, query_attributes, diff, quantity):
    """Function to make a table for an and_or_recommendation (similar) and fill them based on a given query
    ex query: product_id != "%s" and sub_sub_category == "%s" and (selling_price > %s*0.80 and selling_price < %s *1.20)
    will fill the table with products that are in the same sub_sub_category and the price is between 80 & 120 % as current product
    args:
        db: is a PostgresDAO.database object used to connect with the psql database
        table: tablename where the recommendation will be stored. ~~ recommendation name
        pandas_query: the filter query including %s formats where the values of the current product must be formatted
        query_attributes: the attributes needed to format in the pandas query
        diff:         the maximum difference in price for to be 'similar' ex: 0.15 is a maximum price diff of 15%
        quantity:     the quantity of total(!) required recommendations
    ex pandas_query: (product_id != "%s" and sub_sub_category == "%s" and (selling_price > %s*0.80 and selling_price < %s *1.20)
    ex query_attributes: 'product_id, sub_sub_category, selling_price, selling_price'
    """
    # create recommendation table in psql
    query_functions.create_rec_table_query(db, table, "prod_id VARCHAR,")
    # select all from products # todo implement way to FROM profiles with param
    df = psql.read_sql_query("SELECT * FROM products", db._connect())
    db._close_connection()
    # list where all recommendations will be stored so they can be inserted into psql at once.
    all_recommendations = []
    # for all indexes in length of df (will be used to itterate over each product)
    for i in range(0, len(df)):
        # attribute values of the current product
        # ex: ('8532', 'Deodorant') if we ['product_id', 'sub_sub_category'] is used as param for column
        attribute_values = tuple([df[column][i] for column in query_attributes])
        # query over the df find products where a certain filter(filter defined in function call) matches the current product

        # filter all products by price maximum price difference
        recs = df[(df['selling_price'].between(df.iloc[i]['selling_price']*(1-diff), df.iloc[i]['selling_price']*(1+diff)))]
        # filter products by pandas query can be a combination of any attribute
        recs = recs.query(pandas_query % attribute_values)

        # recommendations is list of the n quantity of random recommendable product id's in the available recs
        # if there is not enough recommendations, all the possible recommendations will be used and the missing quantity will be
        # generated by most_similar these are recommendations based on which products are most common given all(!) attributes
        if len(recs) < quantity:
            recommendations = list(recs.sample(n=len(recs))['product_id'])
            recommendations = recommendations + most_similar(recommendations, quantity, df, df.iloc[i], diff)
        else:
            recommendations = list(recs.sample(n=quantity)['product_id'])
        # and insert the current id at recommendations index 0  this will be the PK! this is the id the recommendation is made for
        recommendations.insert(0, df['product_id'][i])
        # make a tuple for insert
        recommendations = tuple(recommendations)
        # insert into all_recommendations
        all_recommendations.append((recommendations, ))
    # fill the recommendation table with all the recommendations
    db.many_update_queries(f"INSERT INTO {table} VALUES %s", all_recommendations)

def start_propositional_logic_recommendation():
    """This function is being called by the control panel to start the propositional_logic_recommendation function"""
    propositional_logic_recommendation(PostgresDAO.db, 'sub_sub_category_price_rec',
                                       recommendation_dict['sub_sub_category'][0],
                                       recommendation_dict['sub_sub_category'][1], 0.15, 4
                                       )

if __name__ == "__main__":
    start_propositional_logic_recommendation()