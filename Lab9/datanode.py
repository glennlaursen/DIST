"""
Aarhus University - Distributed Storage course - Lab 9
HDFS Datanode
"""

from flask import Flask, make_response, request, send_file
import sys, os, io, logging
import random, base64
import urllib.request
import json

import utils

# Read the folder name where chunks should be stored from the first program argument
# (or use the current folder if none was given)
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    # Try to create the folder  
    try:
        os.mkdir('./'+data_folder)
    except FileExistsError as _:
        # OK, the folder exists 
        pass
print("Data folder: %s" % data_folder)

app = Flask(__name__)

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
    return make_response({'message': 'Datanode'})
#

"""
TODO Add the REST endpoints here

Datanode - Store File data
Endpoint: POST /write
Request body: file contents (base64 encoded), file ID and list of remaining datanode addresses
Response: Empty

Datanode - Download file data
Endpoint: GET /read/[fileID]
Request body: Empty
Response: Binary file contents

"""


# Start the Flask app (must be after the endpoint functions)
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
port = 9000 if utils.is_raspberry_pi() else random.randint(9000, 9500)
print("Starting Datanode on port %d\n\n" % port)
app.run(host=host_local_network if utils.is_raspberry_pi() else host_local_computer, port=port)
