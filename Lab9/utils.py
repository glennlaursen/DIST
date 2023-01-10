from flask import g
import sqlite3
import os

def get_db():
    """
    Open a DB connection and attach it to the Flask 'g' object
    """
    if 'db' not in g:
        g.db = sqlite3.connect(
            'namenode.db',
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db
#

def close_db(e=None):
    """
    Close the DB connection
    """
    db = g.pop('db', None)

    if db is not None:
        db.close()
#

def init_db():
    """
    Create the 'file' DB table if it does not exist
    """
    db = sqlite3.connect(
        'namenode.db',
        detect_types=sqlite3.PARSE_DECLTYPES
    )
    db.execute("""CREATE TABLE IF NOT EXISTS file (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    filename TEXT, 
                    size INTEGER, 
                    type TEXT, 
                    created DATETIME DEFAULT CURRENT_TIMESTAMP, 
                    replica_locations TEXT);""")
    db.close()
#

def read_file_by_line(filepath):
    """
    Read the contents of the given text file line by line
    and return it as a list of strings. Empty lines are skipped.

    :param filepath: The text file to read
    :returns: List of strings, one per line
    """
    lines = []
    with open(filepath, "r") as f:
        lines = f.readlines()
        # Remove the newline character at the end of each line
        lines = [line.strip() for line in lines]
        # Remove empty lines
        lines = [line for line in lines if len(line)]
    
    return lines
#

def write_file(data, filename):
    """
    Write the given data to a local file with the given filename

    :param data: A bytes object that stores the file contents
    :param filename: The file name. If not given, a random string is generated
    :return: The file name of the newly written file, or None if there was an error
    """
    
    try:
        # Open filename for writing binary content ('wb')
        # note: when a file is opened using the 'with' statment, 
        # it is closed automatically when the scope ends
        with open('./'+filename, 'wb') as f:
            f.write(data)
            print("File %s saved, size: %d bytes" % (filename, len(data)))
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return False
    
    return True
#

def is_raspberry_pi():
    """
    Returns True if the current platform is a Raspberry Pi, otherwise False.
    """
    return os.uname().nodename == 'raspberrypi'
#