"""
Aarhus University - Distributed Storage course - Lab 9
HDFS Namenode
"""

from flask import Flask, make_response, request
import random
import utils

app = Flask(__name__)
app.teardown_appcontext(utils.close_db)

# Set up DB structure 
utils.init_db()

# Load the datanode IP addresses from datanodes.txt 
DATANODE_ADDRESSES = utils.read_file_by_line('datanodes.txt')
print("Using %d Datanodes:\n%s\n" % (len(DATANODE_ADDRESSES), '\n'.join(DATANODE_ADDRESSES)))


@app.after_request
def add_cors_headers(response):
    """
    Add Cross-Origin Resource Sharing headers to every response.
    Without these the browser does not allow the HDFS webapp to process the response.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Authorization,Content-Type'
    response.headers['access-control-allow-methods'] = 'DELETE, GET, OPTIONS, POST, PUT'
    return response
#

@app.route('/')
def hello():
    return make_response({'message': 'Namenode'})
#


"""
TODO Add the REST endpoints here

Namenode - Add new file
Endpoint: POST /files 
Request body: JSON object with the filename, size and mime type
Response: File ID and 2 random Datanode addresses

Namenode - List files
Endpoint: GET /files
Request body: Empty
Response: List of File objects (ID, filename, size, mime type, list of datanode addresses)
"""


# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_network if utils.is_raspberry_pi() else host_local_computer, port=9000)
