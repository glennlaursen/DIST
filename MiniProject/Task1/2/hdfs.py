import random

import zmq

import messages_pb2
import utils
import logging
from utils import random_string

STORAGE_NODES_NUM = 4

REQUEST_TIMEOUT = 1000


def store_file(file_data: bytearray, k: int, context: zmq.Context):
    """
    Implements storing a file in a delegated manner using 4 storage nodes.

    :param file_data: A bytearray that holds the file contents
    :param k: The number of replicas to store
    :param context: A ZMQ context
    :return: Storage details: { "filename", "n_replicas_k", "replica_locations" }
    """

    # Make sure we can realize max_erasures with 4 storage nodes
    assert (k > 0)
    assert (k <= STORAGE_NODES_NUM)

    # Generate a random name for the file.
    file_data_name = random_string()
    logging.info("Filename for file: %s" % file_data_name)

    task = messages_pb2.storedata_request()

    # Get list of k nodes, that should store the file
    replica_locations = utils.get_k_node_ips(k)
    logging.info("replica_locations for file: %s" % replica_locations)

    # Pick out the first one.
    next_node = replica_locations[0]

    task.filename = file_data_name

    # Put rest of nodes in the message.
    task.replica_locations[:] = replica_locations[1:]

    # Create a REQ/REP socket directly to a node.
    hdfs_send_data_socket = context.socket(zmq.REQ)
    hdfs_send_data_socket.connect('tcp://' + next_node + ':5560')

    hdfs_send_data_socket.send_multipart([
        task.SerializeToString(),
        file_data
    ])

    resp = hdfs_send_data_socket.recv_string()
    logging.info('Received: %s' % resp)

    storage_details = {
        "filename": file_data_name,
        "n_replicas_k": k,
        "replica_locations": replica_locations
    }
    return storage_details


def get_file(storage_details, context: zmq.Context):
    """
    Implements retrieving a file that is stored with RAID 1 using 4 storage nodes.

    :param storage_details: The storage details for a file saved in hdfs method.
    :return: The original file contents
    """
    # Select one filename
    filename = storage_details['filename']
    replica_locations = storage_details['replica_locations']

    # Request both chunks in parallel
    task = messages_pb2.getdata_request()
    task.filename = filename

    # Try the replica locations, one by one, to find an online node.
    for location in replica_locations:
        hdfs_data_req_socket = context.socket(zmq.REQ)
        hdfs_data_req_socket.connect('tcp://' + location + ':5561')
        hdfs_data_req_socket.send(task.SerializeToString())
        logging.info('Trying to get file from: ' + location)

        # Check that node is online
        if (hdfs_data_req_socket.poll(REQUEST_TIMEOUT) & zmq.POLLIN) != 0:
            # Receive file
            result = hdfs_data_req_socket.recv_multipart()
            # First frame: file name (string)
            filename_received = result[0].decode('utf-8')
            # Second frame: data
            file_data = result[1]

            logging.info("Received %s" % filename_received)

            hdfs_data_req_socket.close()

            return file_data
        else:
            # Location is offline, try next one
            logging.warning(location + ' is offline :(')
            hdfs_data_req_socket.close()
            continue
