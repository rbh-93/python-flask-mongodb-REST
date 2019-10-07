import json
import bson
import ast
from bson import ObjectId
from flask import Flask, request
from flask.json import JSONEncoder
from flask_pymongo import PyMongo
from flask_restful import Api, Resource
from RESTServerConfig import rest_server_config_dict

'''
Function to encode '_id' of type ObjectId to 
String returned by MongoDB.
Object of type ObjectId is not JSON serializable 
so encoding has to be done.
'''


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)


app = Flask(__name__)
api = Api(app)

# Connection to MongoDB
app.config["MONGO_URI"] = "mongodb://" + rest_server_config_dict['mongo_address'] + "/" + rest_server_config_dict[
    'db_name']
mongo = PyMongo(app)


# POST endpoint

class PostData(Resource):
    def post(self, collection_name):
        user = mongo.db[collection_name]  # Dynamically create the collection name sent by client
        client_request = request.get_json(force=True)
        user.insert(client_request)  # Insert JSON payload sent by client
        return str(client_request['_id'])  # Return '_id' of collection


# Get all Documents in a particular Collection


class GetAll(Resource):
    def get(self, collection_name):
        result_list = []  # Dict to store the Documents
        cursor = mongo.db[collection_name].find()  # Dynamically choose the Collection sent by client
        for document in cursor:
            encoded_data = JSONEncoder().encode(document)  # Encode the query results to String
            result_list.append(json.loads(encoded_data))  # Update dict by iterating over Documents in the Collection
        return result_list  # Return the result to the client


# Get one Document by ID in a particular Collection

class GetOneById(Resource):
    def get(self, collection_name, objid):
        cursor = mongo.db[collection_name]  # Dynamically choose the Collection sent by client
        document = cursor.find_one_or_404({'_id': bson.ObjectId(oid=str(objid))})  # Find Document by ObjectId
        encoded_data = JSONEncoder().encode(document)  # Encode the query results to String
        return json.loads(encoded_data)  # Return the result to the client


# Execute query on DB


class ExecuteQuery(Resource):
    def get(self, collection_name, client_query):
        result_list = []  # List to store query results
        eval_client_query = ast.literal_eval(client_query)  # Evaluate client_query
        if isinstance(eval_client_query, dict):  # Query contains one part
            cursor = mongo.db[collection_name].find(eval_client_query)  # Execute query
        elif isinstance(eval_client_query, tuple):
            query_list = []
            for items in eval_client_query:  # Query contains multiple parts
                query_list.append(items)  # Extract and append query parts to query_list
            cursor = mongo.db[collection_name].find(*query_list)  # Execute query
        for document in cursor:
            encoded_data = JSONEncoder().encode(document)  # Encode the query results to String
            result_list.append(json.loads(encoded_data))  # Append to list by iterating over Documents
        return result_list  # Return query result to client


api.add_resource(PostData, '/data/<string:collection_name>')  # Route for POST
api.add_resource(GetAll, '/<string:collection_name>')  # Route for GetAll
api.add_resource(GetOneById, '/<string:collection_name>/<objid>')  # Route for GetOneById
api.add_resource(ExecuteQuery, '/query/<string:collection_name>/<string:client_query>')  # Route for ExecuteQuery

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5002)
