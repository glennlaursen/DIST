import time

import zmq

import messages_pb2
import utils
from utils import random_string

STORAGE_NODES_NUM = 4


def store_file_2(file_data: bytearray, k: int, send_task_socket: zmq.Socket, response_socket: zmq.Socket, original_filename: str, measure: bool):
    """
    Implements storing a file with RAID 1 using 4 storage nodes.

    :param file_data: A bytearray that holds the file contents
    :param k: The number of replicas to store
    :param send_task_socket: A ZMQ PUSH socket to the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :param original_filename: The filename put into the http request
    :param measure: Bool. True if replica generation measurements should be made
    :return: Storage Details
    """

    # Make sure we can realize max_erasures with 4 storage nodes
    assert (k > 0)
    assert (k <= STORAGE_NODES_NUM)

    if measure:
        # Start the stopwatch / counter
        t1_start = time.perf_counter()

    # Send k 'store data' Protobuf requests with the file and filename
    for i in range(k):
        task = messages_pb2.storedata_request()
        task.filename = random_string()
        task.replica_locations[:] = [] # Is not used here.
        send_task_socket.send_multipart([
            task.SerializeToString(),
            file_data
        ])

    filenames_and_locations = {}
    # Wait until we receive k responses from the workers
    for task_nbr in range(k):
        resp = response_socket.recv_pyobj()
        print('Received: %s' % resp)
        filenames_and_locations[resp['filename']] = resp['ip']

    if measure:
        # Stop the stopwatch / counter
        t1_stop = time.perf_counter()

        # Log measurement
        f = open("raid1_replica_" + str(k) + "k_" + original_filename + ".csv", "a")
        f.write(str(t1_stop - t1_start) + "\n")
        f.close()

    storage_details = {
        "filenames_and_locations": filenames_and_locations,
        "n_replicas_k": k
    }

    return storage_details


def get_file_2(storage_details, data_req_socket: zmq.Socket, response_socket: zmq.Socket, context: zmq.Context):
    """
    Implements retrieving a file that is stored with RAID 1 using 4 storage nodes.

    :param storage_details: Storage details as return by store_file function.
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :param context: A ZMQ Context
    :return: The original file contents
    """

    # Try each filename one by one, until the file is successfully received.
    for filename, ip in storage_details['filenames_and_locations'].items():
        if not utils.check_node_online(ip, context):
            continue

        task = messages_pb2.getdata_request()
        task.filename = filename
        data_req_socket.send(
            task.SerializeToString()
        )

        # Receive file
        result = response_socket.recv_multipart()
        # First frame: file name (string)
        filename_received = result[0].decode('utf-8')
        # Second frame: data
        file_data = result[1]

        print("Received %s" % filename_received)

        return file_data