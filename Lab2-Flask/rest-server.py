import base64
import os
import sqlite3

from flask import Flask, make_response, g, request, send_file

import utils


def get_db():
    # Connect to the sqlite DB at 'files.db' and store the connection in 'g.db'
    # Re-use the connection if it already exists
    if 'db' not in g:
        g.db = sqlite3.connect('files.db', detect_types=sqlite3.PARSE_DECLTYPES)

    # Enable casting Row objects to Python dictionaries
    g.db.row_factory = sqlite3.Row
    return g.db


def close_db(e=None):
    # Close the DB connection and remove it from the 'g' object
    db = g.pop('db', None)
    if db is not None:
        db.close()


app = Flask(__name__)

# Close the DB connection after serving a request
app.teardown_appcontext(close_db)


@app.route('/')
def hello():
    return make_response({'message': 'Hello World!'})


@app.route('/files', methods=['POST'])
def add_files():
    # Parse the request body as JSON and convert to a Python dictionary
    payload = request.get_json()
    filename = payload.get('filename')
    content_type = payload.get('content_type')

    # Decode the file contents and calculate its original size
    file_data = base64.b64decode(payload.get('contents_b64'))
    size = len(file_data)

    # Store the file locally with a random generated name
    blob_name = utils.write_file(file_data)

    # Insert the File record in the DB
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `blob_name`) VALUES (?,?,?,?)",
        (filename, size, content_type, blob_name))
    db.commit()

    # Return the ID of the new file record with HTTP 201 (Created) status code
    return make_response({"id": cursor.lastrowid}, 201)


@app.route('/files', methods=['GET'])
def list_files():
    # Query the database for all files
    db = get_db()
    cursor = db.execute("SELECT * FROM `file`")

    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    files = cursor.fetchall()

    # Convert files from sqlite3.Row object (which is not JSON-encodable) to
    # a standard Python dictionary simply by casting
    files = [dict(f) for f in files]

    return make_response({"files": files})


@app.route('/files/<int:file_id>', methods=['GET'])
def download_file(file_id):
    if request.method == "HEAD":
        return file_attributes(file_id)

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])

    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()
    if f is None:
        return make_response({"message": "File not found"}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    print("File requested: {}".format(f))

    # Return the binary file contents with the proper Content-Type header.
    return send_file(f['blob_name'], mimetype=f['content_type'])


@app.route('/files/<int:file_id>', methods=['HEAD'])
def file_attributes(file_id):
    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])

    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()

    # Convert to a Python dictionary
    f = dict(f)
    print("File HEAD requested: {}".format(f))

    response = make_response()
    [response.headers.add_header(f'flask_{key}', value) for key, value in f.items()]
    return response


@app.route('/files/<int:file_id>', methods=['DELETE'])
def delete_file(file_id):
    db = get_db()

    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])

    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()

    if f is None:
        return make_response({"message": "File not found in database"}, 404)

    # Convert to a Python dictionary
    f = dict(f)

    if os.path.exists(f.get("blob_name")):
        os.remove(f.get("blob_name"))
    else:
        return make_response({"message": "File not found is filesystem"}, 404)

    cursor = db.execute("DELETE FROM `file` WHERE `id`=?", [file_id])

    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    db.commit()

    return make_response("", 204)


app.run(host="localhost", port=9000)
