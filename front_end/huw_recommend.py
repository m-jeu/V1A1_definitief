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
    def freq_combined_sub_sub_category_decision(db, count, productids, budget, profileid):
        """Function to make live recommendations based on the user's historical spending patterns and which sub_sub categories has been often combined with the
        product ids in the shoppingcart. If there is not enough historical data about a profile only a recommendation is made based on the most frequently combined
        sub_sub_category.
        args:
            db: PostgresDAO.db object
            count: the amount of recommendations requested
            productids: a string of product id's this can be 1 or multiple ids
            budget: The budget rules that determine how many standard deviations lower/higher the price range should be
            profileid: The profileid of the current customer
        Returns:
            a response to the front-end which includes which product_id's to recommend"""
        # split the productids string to make it a list
        productids = productids.split('| ')
        productids = tuple(productids)
        # find the budget preference of the current user that's is stored in PostgreSQL budget, normal or luxury
        budget_preference = db.query("SELECT budget_preference from profiles where profile_id = %s;",
                                     (profileid,),
                                     expect_return=True)[0][0]
        # list of all the recommendations where a the required amount of recommendations will be sampled
        all_recommendations = []
        # if there is not enough data about the profile to determine the budget preference continue
        if budget_preference is None:
            for productid in productids:
                # voor elk product in product id
                recommendations = db.query(
                    f"""SELECT pro1, pro2, pro3, pro4 FROM freq_combined_sub_sub_category where prod_id = %s""",
                    (productid,), expect_return=True)[0]
                for rec in recommendations:
                    all_recommendations.append(rec)
            # return the required amount of recommendations from all_recommendations
            return tuple(random.sample(all_recommendations, count)), 200
        else:
            for productid in productids:
                # fetch most frequenly combined sub_sub_category with the current productid from the db
                most_combined_sub_sub = \
                db.query(f"""SELECT sub_sub_category from freq_combined where product_id = %s ;""", (productid,),
                                     expect_return=True)[0][0]
                # find the average price and standard deviation of that sub_sub_category
                average_price, std = db.query(
                    f"""SELECT average_price, standard_deviation FROM sub_sub_category_price_information WHERE sub_sub_category = %s """,
                    (most_combined_sub_sub,), expect_return=True)[0]
                # find products in the most frequently combined sub_sub_category where selling price is between a certain range according to the profile's
                # budget preference
                # the dictionary param budget determines howmany std deviations higher or lower the selling price should be from the average price
                # with a max price difference of 15%
                recommendations = db.query(
                    f"""SELECT product_id FROM products where sub_sub_category = %s and selling_price between %s and %s""",
                    (most_combined_sub_sub,
                     (average_price + (std * budget[budget_preference])) * 0.85,
                     (average_price + (std * budget[budget_preference])) * 1.15),
                    expect_return=True)
                for rec in recommendations:
                    all_recommendations.append(rec[0])
            # return the required amount of recommendations from all_recommendations
            return tuple(random.sample(all_recommendations, count)), 200

    def get(self, count, productid, rec_type, profileid):
        """ This function represents the handler for GET requests coming in
        through the API. receives request from the front end with which recommendation_rule a recommendation should be made and the
        productids/profileid of the current session in the front_end.
        args:
            count: amount recommendations requested.
            productid: productid(s) depending from where the request is send (productpage, shoppingcart etc).
            rec_type: string of the recommendation rule that must be used to fulfill the recommendations.
            profileid: profileid of the current session.
        Returns:
            If no errors occured return status code 200
            If the requested count != 4, returns status code 418"""
        if count != 4:
            return ([], 418)
        if rec_type == 'popularity_rec':
            return PostgresDAO.db.query("SELECT * FROM popularity_recommendation;", expect_return=True)[0], 200
        elif rec_type == 'freq_combined_sub_sub_category':
            return Recom.freq_combined_sub_sub_category_decision(PostgresDAO.db, count, productid, {'Luxury': 0.5,
                  'Normal': 0,
                  'Budget': -0.5}, profileid)
        elif rec_type == 'sub_sub_category_price_rec':
            return PostgresDAO.db.query("""SELECT pro1, pro2, pro3, pro4 FROM sub_sub_category_price_rec where prod_id = %s""", (productid,), expect_return=True)[0], 200

# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<int:count>/<string:productid>/<string:rec_type>/<string:profileid>")