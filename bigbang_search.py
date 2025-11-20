"""
Simple Flask application that provides a search form and displays results
retrieved from an Elasticsearch instance.

Author: Nicole Gottschall
Template provided by: Prof. Maik Thiele, HTW Dresden (Faculty of Computer Science/ Mathematics)

Description:
- renders a landing page with a search form ('/')
- processes form submissions and forwards results to a results page ('/search/results')
- connects to an Elasticsearch instance running on localhost:9200
"""

import os
from dotenv import load_dotenv
from flask import Flask, render_template, request
from elasticsearch import Elasticsearch

# load environment variables
load_dotenv()
ELASTIC_ADDRESS = os.getenv("ELASTIC_ADDRESS")
INDEX_NAME = os.getenv("INDEX_NAME")

# initialize flask application and configure template directory
app = Flask(__name__, template_folder='./templates')
# initialize elasticsearch client
es_client = Elasticsearch(ELASTIC_ADDRESS)

# landing page: renders the search form
@app.route('/')
def home():
    # fetch all documents from index
    result = es_client.search(
        index=INDEX_NAME,
        body = {
            "query": {"match_all": {}},
            "size": 1000
        }
    )

    # extract the actual documents
    documents = [doc["_source"] for doc in result["hits"]["hits"]]

    return render_template('search.html', documents=documents)

# results page: handles form submissions and renders result template
@app.route('/search/results', methods=['GET', 'POST'])
def search_request():
    # extract search term from POST request
    search_term = request.form["input"]

    # TODO: perform elasticsearch query
    # res = es.search(...)

    return render_template('results.html', res=res )

# application entry point
if __name__ == '__main__':
    app.secret_key = 'mysecret'
    app.run(host='localhost', port=5000)