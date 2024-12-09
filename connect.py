from elasticsearch import Elasticsearch

# Connecting to Elasticsearch, in order to not use the same code everytime that i want to connect to Elastic search, I just import connect.py in the script that it will be needed it
es = Elasticsearch("http://localhost:9200")