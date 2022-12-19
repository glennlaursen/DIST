import zmq

import messages_pb2
import math
import random
import logging

from utils import random_string

STORAGE_NODES_NUM = 4

def store_file(file_data: bytearray, k: int, send_task_socket: zmq.Socket, response_socket: zmq.Socket):
    """
    Implements storing a file with RAID 1 using 4 storage nodes.

    :param file_data: A bytearray that holds the file contents
    :param k: The number of replicas to store
    :param send_task_socket: A ZMQ PUSH socket to the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: A list of the random generated chunk names, e.g. (c1,c2), (c3,c4)
    """

    # Make sure we can realize max_erasures with 4 storage nodes
    assert (k > 0)
    assert (k <= STORAGE_NODES_NUM)

    # Generate k random filenames
    file_data_names = [random_string() for _ in range(k)]
    logging.info("Filenames for file: %s" % file_data_names)

    # Send k 'store data' Protobuf requests with the file and filename
    for name in file_data_names:
        task = messages_pb2.storedata_request()
        task.filename = name
        send_task_socket.send_multipart([
            task.SerializeToString(),
            file_data
        ])

    # Wait until we receive k responses from the workers
    for task_nbr in range(k):
        resp = response_socket.recv_string()
        logging.info('Received: %s' % resp)

    storage_details = {
        "filenames": file_data_names,
        "n_replicas_k": k
    }

    # Return storage details
    return storage_details


def get_file(storage_details, data_req_socket: zmq.Socket, response_socket: zmq.Socket):
    """
    Implements retrieving a file that is stored with RAID 1 using 4 storage nodes.

    :param storage_details: Storage details as return by store_file function.
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: The original file contents
    """

    # Try each filename one by one, until the file is successfully received.
    for filename in storage_details['filenames']:
        task = messages_pb2.getdata_request()
        task.filename = filename
        data_req_socket.send(
            task.SerializeToString()
        )

        if (response_socket.poll(1000) & zmq.POLLIN) != 0:
            # Receive file
            result = response_socket.recv_multipart()
            # First frame: file name (string)
            filename_received = result[0].decode('utf-8')
            # Second frame: data
            file_data = result[1]

            logging.info("Received %s" % filename_received)

            return file_data
        else:
            logging.warning('Location that stores file: ' + filename + ', is offline :(')
            continue