import random
import string
import platform

import messages_pb2
import zmq
import logging


def random_string(length=8):
    """
    Returns a random alphanumeric string of the given length. 
    Only lowercase ascii letters and numbers are used.

    :param length: Length of the requested random string 
    :return: The random generated string
    """
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
        filename = random_string(8)
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


def check_nodes(heartbeat_request_socket, response_socket, n_nodes):
    connected_nodes_fragments = {}
    connected_nodes_ip = []

    task = messages_pb2.heartbeat_request()
    heartbeat_request_socket.send_multipart([b"all_nodes", task.SerializeToString()])

    # Check to see which nodes are alive
    for i in range(n_nodes):
        if response_socket.poll(500, zmq.POLLIN):
            msg = response_socket.recv()
            response = messages_pb2.heartbeat_response()
            response.ParseFromString(msg)
            connected_nodes_fragments[response.node_id] = response.fragments
            connected_nodes_ip.append(response.node_ip)

    return connected_nodes_fragments, connected_nodes_ip


def get_fragments(heartbeat_request_socket, response_socket, n_nodes):
    connected_nodes_fragments = {}


def get_connected_nodes(heartbeat_request_socket: zmq.Socket, response_socket: zmq.Socket, n_nodes):
    connected_nodes_ip = []

    task = messages_pb2.heartbeat_request()
    heartbeat_request_socket.send_multipart([b"all_nodes", task.SerializeToString()])

    # Check to see which nodes are alive
    for i in range(n_nodes):
        if (response_socket.poll(500) & zmq.POLLIN) != 0:
            msg = response_socket.recv_string()
            connected_nodes_ip.append(msg)

    return connected_nodes_ip


def is_raspberry_pi():
    """
    Returns True if the current platform is a Raspberry Pi, otherwise False.
    """
    return 'raspberyypi' in platform.uname().node


def is_docker():
    return 'WSL' in platform.uname().release


def create_logger(name, path):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    f_handler = logging.FileHandler(path)
    f_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(f_handler)
    return logger
