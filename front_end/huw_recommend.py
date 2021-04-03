from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from V1A1_definitief.database import PostgresDAO

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

    def get(self, count, profileid, rec_type):
        """ This function represents the handler for GET requests coming in
        through the API. It currently queries the simple_recommendation table
    in PostgreSQL, and returns the 4 most popular products. If the requested count != 4,
    returns status code 418"""
        if count != 4:
            return ([], 418)
        if rec_type == 'popularity_rec':
            return PostgresDAO.db.query("SELECT * FROM popularity_recommendation;", expect_return=True)[0], 200
        elif rec_type == 'sub_sub_category_price_rec':
            return PostgresDAO.db.query("SELECT pro1, pro2, pro3, pro4 FROM sub_sub_category_price_rec where prod_id = %s", (profileid,), expect_return=True)[0], 200

# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<int:count>/<string:profileid>/<string:rec_type>")