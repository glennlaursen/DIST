"""
Aarhus University - Distributed Storage course - Lab 7

REST API
"""
import io  # For sending binary data in a HTTP response
import logging
import sqlite3
import sys
import time  # For waiting a second for ZMQ connections

import zmq  # For ZMQ
from flask import Flask, make_response, g, request, send_file

import hdfs
from utils import is_raspberry_pi, is_docker

import json


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(
            'files.db',
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()


# Initiate ZMQ sockets
context = zmq.Context()

n_replicas_k = data_folder = sys.argv[1] if len(sys.argv) > 1 else 3

# Wait for all workers to start and connect. 
time.sleep(1)
logging.info("Listening to ZMQ messages on tcp://*:5558 and tcp://*:5561")

# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)
# Close the DB connection after serving the request
app.teardown_appcontext(close_db)


@app.route('/')
def hello():
    return make_response({'message': 'Hello World!'})


@app.route('/files', methods=['GET'])
def list_files():
    db = get_db()
    cursor = db.execute("SELECT * FROM `file`")
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    files = cursor.fetchall()
    # Convert files from sqlite3.Row object (which is not JSON-encodable) to 
    # a standard Python dictionary simply by casting
    files = [dict(file) for file in files]

    return make_response({"files": files})


@app.route('/files/<int:file_id>', methods=['GET'])
def download_file(file_id):
    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    logging.info("File requested: {}".format(f['filename']))

    # Parse the storage details JSON string
    storage_details = json.loads(f['storage_details'])

    file_data = hdfs.get_file(storage_details, context)

    return send_file(io.BytesIO(file_data), mimetype=f['content_type'])


# HTTP HEAD requests are served by the GET endpoint of the same URL,
# so we'll introduce a new endpoint URL for requesting file metadata.
@app.route('/files/<int:file_id>/info', methods=['GET'])
def get_file_metadata(file_id):
    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    logging.info("File: %s" % f)

    return make_response(f)


@app.route('/files/<int:file_id>', methods=['DELETE'])
def delete_file(file_id):
    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    logging.info("File to delete: %s" % f)

    # TODO Delete all chunks from the Storage Nodes

    # TODO Delete the file record from the DB

    # Return empty 200 Ok response
    return make_response('TODO: implement this endpoint', 404)


@app.route('/files_mp', methods=['POST'])
def add_files_multipart():
    # Flask separates files from the other form fields
    payload = request.form
    files = request.files

    # Make sure there is a file in the request
    if not files or not files.get('file'):
        logging.error("No file was uploaded in the request!")
        return make_response("File missing!", 400)

    # Reference to the file under 'file' key
    file = files.get('file')
    # The sender encodes a file name and type together with the file contents
    filename = file.filename
    content_type = file.mimetype
    # Load the file contents into a bytearray and measure its size
    data = bytearray(file.read())
    size = len(data)
    logging.info("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))

    # Read the requested storage mode from the form (default value: 'raid1')
    storage_mode = payload.get('storage', 'raid1')
    logging.info("Storage mode: %s" % storage_mode)

    storage_details = hdfs.store_file(data, n_replicas_k, context)

    # Insert the File record in the DB
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `storage_details`) VALUES (?,?,?,?,?)",
        (filename, size, content_type, storage_mode, json.dumps(storage_details))
    )
    db.commit()

    logging.info('Storage details: (filename: ' + filename + ', size: ' + str(size) + ', content_type: ' + content_type
          + ', storage_mode: ' + storage_mode + ', storage_details: ' + json.dumps(storage_details))

    return make_response({"id": cursor.lastrowid}, 201)


# Reed-Solomon repair goes here
# TO BE DONE

# Automated RS repair goes here
# TO BE DONE

@app.errorhandler(500)
def server_error(e):
    logging.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)


# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost"  # Listen for connections on the local computer
host_local_network = "0.0.0.0"  # Listen for connections on the local network
app.run(host=host_local_network if is_raspberry_pi() or is_docker() else host_local_computer, port=9000)
