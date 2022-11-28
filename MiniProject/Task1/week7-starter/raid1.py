import messages_pb2
import math
import random

from utils import random_string

STORAGE_NODES_NUM = 4

def store_file(file_data, k, send_task_socket, response_socket):
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

    # Generate two random chunk names for each half
    file_data_names = [random_string() for _ in range(k)]
    print("Filenames for file: %s" % file_data_names)

    # Send k 'store data' Protobuf requests with the file and filename
    for name in file_data_names:
        task = messages_pb2.storedata_request()
        task.filename = name
        send_task_socket.send_multipart([
            task.SerializeToString(),
            file_data
        ])

    # Wait until we receive 4 responses from the workers
    for task_nbr in range(k):
        resp = response_socket.recv_string()
        print('Received: %s' % resp)

    # Return the chunk names of each replica
    return file_data_names


def get_file(filenames, data_req_socket, response_socket):
    """
    Implements retrieving a file that is stored with RAID 1 using 4 storage nodes.

    :param filenames: List of names that store the file
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: The original file contents
    """
    # Select one filename
    filename = filenames[random.randint(0, len(filenames) - 1)]

    # Request both chunks in parallel
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
