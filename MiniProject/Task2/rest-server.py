"""
Aarhus University - Distributed Storage course - Lab 6

REST API + RAID Controller
"""
import flask
from flask import Flask, make_response, g, request, send_file
import sqlite3
import base64
import random
import string
import logging

import zmq # For ZMQ
import time # For waiting a second for ZMQ connections
import math # For cutting the file in half
import messages_pb2 # Generated Protobuf messages
import io # For sending binary data in a HTTP response

import raid1
import reedsolomon

from utils import is_raspberry_pi, is_docker, create_logger

logger_storing_all = create_logger("rs_storing_all", "log_rs_st_all.log")
logger_storing_server = create_logger("rs_storing_ser", "log_rs_st_ser.log")

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

# Socket to send tasks to Storage Nodes
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind("tcp://*:5557")

# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind("tcp://*:5558")

# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind("tcp://*:5559")

# Publisher socket for fragment repair broadcasts
repair_socket = context.socket(zmq.PUB)
repair_socket.bind("tcp://*:5560")

# Socket to receive repair messages from Storage Nodes
repair_response_socket = context.socket(zmq.PULL)
repair_response_socket.bind("tcp://*:5561")

heartbeat_socket = context.socket(zmq.PUB)
heartbeat_socket.bind("tcp://*:5562")

# Wait for all workers to start and connect. 
time.sleep(1)
print("Listening to ZMQ messages on tcp://*:5558 and tcp://*:5561")

# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)

# Stop Flask from logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Close the DB connection after serving the request
app.teardown_appcontext(close_db)

@app.route('/')
def hello():
    return make_response({'message': 'Hello World!'})

@app.route('/files',  methods=['GET'])
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
#

@app.route('/files/<int:file_id>',  methods=['GET'])
def download_file(file_id):

    file_data = None

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    print("File requested: {}".format(f['filename']))

    # Parse the storage details JSON string
    import json
    storage_details = json.loads(f['storage_details'])

    if f['storage_mode'] == 'raid1':

        part1_filenames = storage_details['part1_filenames']
        part2_filenames = storage_details['part2_filenames']

        file_data = raid1.get_file(
            part1_filenames,
            part2_filenames,
            data_req_socket,
            response_socket
        )

    elif f['storage_mode'] == 'erasure_coding_rs':

        coded_fragments = storage_details['coded_fragments']
        max_erasures = storage_details['max_erasures']
        type = storage_details['type']

        if type == 1:

            file_data = reedsolomon.get_file(
                coded_fragments,
                max_erasures,
                f['size'],
                data_req_socket,
                heartbeat_socket,
                response_socket
            )
        elif type == 2:
            file_data = reedsolomon.get_file_delegate(
                coded_fragments,
                max_erasures,
                f['size'],
                data_req_socket,
                heartbeat_socket,
                response_socket,
                context
            )

    if file_data is None:
        return make_response('Something went wrong, please try again', 404)

    if isinstance(file_data, str):
        return make_response(file_data, 404)

    else:
        return send_file(io.BytesIO(file_data), mimetype=f['content_type'])

#

# HTTP HEAD requests are served by the GET endpoint of the same URL,
# so we'll introduce a new endpoint URL for requesting file metadata.
@app.route('/files/<int:file_id>/info',  methods=['GET'])
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
    print("File: %s" % f)

    return make_response(f)
#

@app.route('/files/<int:file_id>',  methods=['DELETE'])
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
    print("File to delete: %s" % f)

    # TODO Delete all chunks from the Storage Nodes

    # TODO Delete the file record from the DB

    # Return empty 200 Ok response
    return make_response('TODO: implement this endpoint', 404)
#

@app.route('/files_mp', methods=['POST'])
def add_files_multipart():

    t1 = time.perf_counter()

    # Flask separates files from the other form fields
    payload = request.form
    files = request.files

    # Make sure there is a file in the request
    if not files or not files.get('file'):
        logging.error("No file was uploaded in the request!")
        return make_response("File missing!", 400)

    # Reference to the file under 'file' key
    file = files.get('file')
    # The sender encodes the file name and type together with the file contents
    filename = file.filename
    content_type = file.mimetype
    # Load the file contents into a bytearray and measure its size
    data = bytearray(file.read())
    size = len(data)
    print("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))

    # Read the requested storage mode from the form (default value: 'raid1')
    storage_mode = payload.get('storage', 'raid1')
    print("Storage mode: %s" % storage_mode)
    measure = payload.get('measure', 'false')

    if storage_mode == 'raid1':
        file_data_1_names, file_data_2_names = raid1.store_file(data, send_task_socket, response_socket)

        storage_details = {
            "part1_filenames": file_data_1_names,
            "part2_filenames": file_data_2_names
        }

    elif storage_mode == 'erasure_coding_rs':
        # Reed Solomon code
        # Parse max_erasures (everything is a string in request.form, 
        # we need to convert to int manually), set default value to 1
        max_erasures = int(payload.get('max_erasures', 1))
        type = int(payload.get('type', 1))

        if max_erasures > 2:
            return make_response('max_erasures cannot exceed 2, please try again', 400)
        else:
            print("Max erasures: %d" % (max_erasures))
            fragment_names = None
            if type == 1:
                # Store the file contents with Reed Solomon erasure coding
                fragment_names = reedsolomon.store_file(data, max_erasures, send_task_socket, response_socket)
            elif type == 2:
                # Store the file, delegating encoding to random node
                fragment_names = reedsolomon.store_file_delegate(data, max_erasures, heartbeat_socket,
                                                                 response_socket, context)
                t_server_done = time.perf_counter()
                if measure == 'true':
                    # Wait for all fragments to be stored
                    timer_socket = context.socket(zmq.REP)
                    timer_socket.bind("tcp://*:5545")
                    resp = timer_socket.recv_string()
                    print(resp)

            if fragment_names is not None:
                storage_details = {
                    "coded_fragments": fragment_names,
                    "max_erasures": max_erasures,
                    "type": type
                }
            else:
                return make_response("Something went wrong, try again", 400)

    else:
        logging.error("Unexpected storage mode: %s" % storage_mode)
        return make_response("Wrong storage mode", 400)


    t2 = time.perf_counter()
    duration_all = t2-t1
    duration_server = t_server_done-t1
    logger_storing_all.info(str(len(data)) + ", " + str(duration_all))
    logger_storing_server.info(str(len(data)) + ", " + str(duration_server))



    # Insert the File record in the DB
    import json
    db = get_db()

    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `storage_details`) VALUES (?,?,?,?,?)",
        (filename, size, content_type, storage_mode, json.dumps(storage_details))
    )
    db.commit()

    return make_response({"id": cursor.lastrowid }, 201)
#

@app.route('/files', methods=['POST'])
def add_files():
    payload = request.get_json()
    filename = payload.get('filename')
    content_type = payload.get('content_type')
    file_data = base64.b64decode(payload.get('contents_b64'))
    size = len(file_data)

    file_data_1_names, file_data_2_names = raid1.store_file(file_data, send_task_socket, response_socket)

    # Insert the File record in the DB
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `part1_filenames`, `part2_filenames`) VALUES (?,?,?,?,?)",
        (filename, size, content_type, ','.join(file_data_1_names), ','.join(file_data_2_names))
    )
    db.commit()

    # Return the ID of the new file record with HTTP 201 (Created) status code
    return make_response({"id": cursor.lastrowid }, 201)
#


@app.route('/services/rs_repair',  methods=['GET'])
def rs_repair():
    #Retrieve the list of files stored using Reed-Solomon from the database
    db = get_db()
    cursor = db.execute("SELECT `id`, `storage_details`, `size` FROM `file` WHERE `storage_mode`='erasure_coding_rs'")
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    rs_files = cursor.fetchall()
    rs_files = [dict(file) for file in rs_files]

    fragments_missing, fragments_repaired = reedsolomon.start_repair_process(rs_files,
                                                                             repair_socket,
                                                                             repair_response_socket)

    return make_response({"fragments_missing": fragments_missing,
                          "fragments_repaired": fragments_repaired})
#


def rs_automated_repair():
    print("Running automated Reed-Solomon repair process")
    with app.app_context():
        rs_repair()
#


@app.errorhandler(500)
def server_error(e):
    logging.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)


# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_network if is_raspberry_pi() or is_docker() else host_local_computer, port=9000)
