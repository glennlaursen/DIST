"""
Aarhus University - Distributed Storage course - Lab 4

REST Server, starter template for Week 4
"""
import logging
from base64 import b64decode

import zmq  # For ZMQ
import time  # For waiting a second for ZMQ connections
import math  # For cutting the file in half
import random  # For selecting a random half when requesting chunks
import messages_pb2  # Generated Protobuf messages
import io  # For sending binary data in a HTTP respons
from flask import Flask, make_response, g, request, send_file
import sqlite3

"""
Utility Functions
"""


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


def random_string(length=8):
    """
    Returns a random alphanumeric string of the given length. 
    Only lowercase ascii letters and numbers are used.

    :param length: Length of the requested random string 
    :return: The random generated string
    """
    import random, string
    return ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for n in range(length)])


def write_file(data, filename=None):
    """
    Write the given data to a local file with the given filename

    :param data: A bytes object that stores the file contents
    :param filename: The file name. If not given, a random string is generated
    :return: The file name of the newly written file, or None if there was an error
    """
    if not filename:
        # Generate random filename
        filename = random_string(length=8)
        # Add '.bin' extension
        filename += ".bin"

    try:
        # Open filename for writing binary content ('wb')
        # note: when a file is opened using the 'with' statment, 
        # it is closed automatically when the scope ends
        with open('./' + filename, 'wb') as f:
            f.write(data)
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None

    return filename


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
# Wait for all workers to start and connect.
time.sleep(1)
print("Listening to ZMQ messages on tcp://*:5558")

"""
REST API
"""

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


#

@app.route('/files/<int:file_id>', methods=['GET'])
def download_file(file_id):
    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()
    # Convert to a Python dictionary
    f = dict(f)

    # Select one chunk of each half
    part1_filenames = f['part1_filenames'].split(',')
    part2_filenames = f['part2_filenames'].split(',')
    part1_filename = part1_filenames[random.randint(0, len(part1_filenames) - 1)]
    part2_filename = part2_filenames[random.randint(0, len(part2_filenames) - 1)]

    # Request both chunks in parallel
    task1 = messages_pb2.getdata_request()
    task1.filename = part1_filename
    data_req_socket.send(task1.SerializeToString())

    task2 = messages_pb2.getdata_request()
    task2.filename = part2_filename
    data_req_socket.send(task2.SerializeToString())

    # Receive both chunks and insert them to
    file_data_parts = [None, None]
    for _ in range(2):
        result = response_socket.recv_multipart()

        # First frame: file name (string)
        filename_received = result[0].decode('utf-8')

        # Second frame: data
        chunk_data = result[1]

        print(f"Received {filename_received}")

        if filename_received == part1_filename:
            # The first part was received
            file_data_parts[0] = chunk_data
        else:
            # The second part was received
            file_data_parts[1] = chunk_data

    print("Both chunks received successfully")

    # Combine the parts and serve the file
    file_data = file_data_parts[0] + file_data_parts[1]
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
    print("File: %s" % f)

    return make_response(f)


#

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
    print("File to delete: %s" % f)

    # Delete the file contents with os.remove()
    from os import remove
    remove(f['blob_name'])

    # Delete the file record from the DB
    db.execute("DELETE FROM `file` WHERE `id`=?", [file_id])
    db.commit()

    # Return empty 200 Ok response
    return make_response('')


#


@app.route('/files', methods=['POST'])
def add_files():
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
    file_data = file.read()
    size = len(file_data)
    print("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))

    # RAID 1: cut the file in half and store both halves 2x
    file_data_1 = file_data[:math.ceil(size / 2.0)]
    file_data_2 = file_data[math.ceil(size / 2.0):]

    # Generate two random chunk names for each half
    file_data_1_names = [random_string(8), random_string(8)]
    file_data_2_names = [random_string(8), random_string(8)]
    print(f"Filenames for part 1: {file_data_1_names}")
    print(f"Filenames for part 2: {file_data_2_names}")

    # Send 2 'store data' Protobuf requests with the first half and chunk names
    for name in file_data_1_names:
        task = messages_pb2.storedata_request()
        task.filename = name
        send_task_socket.send_multipart([
            task.SerializeToString(),
            file_data_1
        ])

    # Send 2 'store data' Protobuf requests with the second half and chunk names
    for name in file_data_2_names:
        task = messages_pb2.storedata_request()
        task.filename = name
        send_task_socket.send_multipart([
            task.SerializeToString(),
            file_data_2
        ])

    # Wait until we receive 4 responses from the workers
    for task_nbr in range(4):
        resp = response_socket.recv_string()
        print(f"Received: {resp}")

    # At this point all chunks are stored, insert the File record in the DB

    # Insert the File record in the DB
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `part1_filenames`, `part2_filenames`) VALUES(?, ?, ?, ?, ?)",
        (filename, size, content_type, ','.join(file_data_1_names), ','.join(file_data_2_names)))
    db.commit()

    # Return the ID of the new file record with HTTP 201 (Created) status code
    return make_response({"id": cursor.lastrowid}, 201)


#


@app.errorhandler(500)
def server_error(e):
    from logging import exception
    exception("Internal error: %s", e)

    return make_response({"error": str(e)}, 500)


# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost"  # Listen for connections on the local computer
host_local_network = "0.0.0.0"  # Listen for connections on the local network
app.run(host=host_local_computer, port=9000)
