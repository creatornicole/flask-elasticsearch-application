"""
Elasticsearch Bulk Indexing Script

This script reads a JSON file containing documents and indexes them into an  Elasticsearch instance using the bulk API.

Usage:
    python indexer_elastic.py <document_filename>

Modules:
    - sys: For reading command-line arguments
    - time: for measuring indexing duration
    - json: for parsing JSON documents
    - elasticsearch: for connecting to Elasticsearch and indexing data
    - elasticsearch.helpers.bulk: for efficient bulk indexing of documents

Configuration:
    ELASTIC_ADDRESS (str): the URL of the Elasticsearch instance (default: "http://localhost:9200")
    INDEX_NAME (str): the name of the Elasticsearch index to write documents to (default: "bigbang")

Functions:
    index_documents(documents_filename, index_name, es_client):
        Reads documents from a specified JSON file, assigns a unique "_id" to each document,
        and indexes them into the given Elasticsearch index using the bulk API.
        Print the number of successfully indexed documents and any failures.

    main():
        Parses the JSON filename from command-line arguments, initializes the Elasticsearch client,
        measures the indexing time, and calls 'index_documents'

Script Execution:
    The script is executed via the command line and requires one argument: the path to a JSON file
    containing the documents to index

    example:
        python indexer_elastic.py data/bbt_episodes.json

Reference: https://sease.io/2023/09/how-to-use-python-api-to-index-json-data-in-elasticsearch.html
"""

import sys
import os
import time
import json
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# load environment variables
load_dotenv()
ELASTIC_ADDRESS = os.getenv("ELASTIC_ADDRESS")
INDEX_NAME = os.getenv("INDEX_NAME")

def index_documents(documents_filename, index_name, es_client):
    index = 0
    # open the file containing the JSON data to index
    with open(documents_filename, "r") as json_file:
        json_data = json.load(json_file)
        documents = []
        for doc in json_data:
            doc["_id"] = index
            documents.append(doc)
            index = index + 1
        indexing = bulk(es_client, documents, index=index_name, chunk_size=100)
        print("Success - %s , Failed - %s" % (indexing[0], len(indexing[1])))

def main():
    documents_filename = sys.argv[1]

    # initialize elasticsearch client
    es_client = Elasticsearch(ELASTIC_ADDRESS)

    initial_time = time.time()
    index_documents(documents_filename, INDEX_NAME, es_client)
    print("Finished")
    finish_time = time.time()
    print("Documents indexed in {:f} seconds\n".format(finish_time - initial_time))

if __name__ == "__main__":
    main()