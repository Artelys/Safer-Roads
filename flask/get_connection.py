from elasticsearch import Elasticsearch

def get_connection():
    # Elastic Search parameters
    #AUTH = ('elastic', 'changeme')
    PORT = 9200
    HOST = "elastic"
    # change this to use AUTH
    #return Elasticsearch([{'host': HOST, 'port': PORT}], http_auth=AUTH)
    return Elasticsearch([{'host': HOST, 'port': PORT}])

