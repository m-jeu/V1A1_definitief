import random
from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from V1A1_definitief.database import PostgresDAO
import pandas.io.sql as psql
import itertools

app = Flask(__name__)
api = Api(app)

# We define these variables to (optionally) connect to an external MongoDB
# instance.
envvals = ["MONGODBUSER","MONGODBPASSWORD","MONGODBSERVER"]
dbstring = 'mongodb+srv://{0}:{1}@{2}/test?retryWrites=true&w=majority'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the connection to the database outside of the
# Recom class.
load_dotenv()
if os.getenv(envvals[0]) is not None:
    envvals = list(map(lambda x: str(os.getenv(x)), envvals))
    client = MongoClient(dbstring.format(*envvals))
else:
    client = MongoClient()
database = client.huwebshop

class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. At the moment, the API simply returns a random set of products
    to recommend."""
    @staticmethod
    def freq_combined_sub_sub_category_decision(db, count, productid, rec_type, profileid):
        budget = {'Luxury': 0.5,
                  'Normal': 0,
                  'Budget': -0.5}
        productid = productid.split('| ')
        productid = tuple(productid)
        budget_preference = db.query("SELECT budget_preference from profiles where profile_id = %s;",
                                     (profileid,),
                                     expect_return=True)[0][0]
        with open('output.txt', 'w') as output:
            output.write(f'{profileid} budget {budget_preference}')
        if budget_preference is None:
            return PostgresDAO.db.query(
                f"""SELECT pro1, pro2, pro3, pro4 FROM freq_combined_sub_sub_category where prod_id in %s order by random() limit {count};""",
                (productid,), expect_return=True)[0], 200
        else:
            all_recommendations = []
            for id in productid:
                # voor elk product in product id
                most_combined_sub_sub = \
                PostgresDAO.db.query(f"""SELECT sub_sub_category from freq_combined where product_id = %s ;""", (id,),
                                     expect_return=True)[0][0]
                # query de meest gecombineerde sub_sub_category
                average_price, std = PostgresDAO.db.query(
                    f"""SELECT average_price, standard_deviation FROM sub_sub_category_price_information WHERE sub_sub_category = %s """,
                    (most_combined_sub_sub,), expect_return=True)[0]
                recommendations = PostgresDAO.db.query(
                    f"""SELECT product_id FROM products where sub_sub_category = %s and selling_price between %s and %s""",
                    (most_combined_sub_sub,
                     (average_price + (std * budget[budget_preference])) * 0.85,
                     (average_price + (std * budget[budget_preference])) * 1.15),
                    expect_return=True)
                for rec in recommendations:
                    all_recommendations.append(rec[0])
            return tuple(random.sample(all_recommendations, count)), 200



    def get(self, count, productid, rec_type, profileid):
        """ This function represents the handler for GET requests coming in
        through the API. It currently queries the simple_recommendation table
    in PostgreSQL, and returns the 4 most popular products. If the requested count != 4,
    returns status code 418"""
        if count != 4:
            return ([], 418)
        if rec_type == 'popularity_rec':
            return PostgresDAO.db.query("SELECT * FROM popularity_recommendation;", expect_return=True)[0], 200
        elif rec_type == 'freq_combined_sub_sub_category':
            return Recom.freq_combined_sub_sub_category_decision(PostgresDAO.db, count, productid, rec_type, profileid)

        elif rec_type == 'sub_sub_category_price_rec':
            return PostgresDAO.db.query("""SELECT pro1, pro2, pro3, pro4 FROM sub_sub_category_price_rec where prod_id = %s""", (productid,), expect_return=True)[0], 200

# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<int:count>/<string:productid>/<string:rec_type>/<string:profileid>")