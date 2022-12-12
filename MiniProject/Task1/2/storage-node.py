"""
Aarhus University - Distributed Storage course - Lab 7

Storage Node
"""
import os
import sys

import zmq

import messages_pb2
from utils import random_string, write_file, is_raspberry_pi

# Read the folder name where chunks should be stored from the first program argument
# (or use the current folder if none was given)
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    # Try to create the folder  
    try:
        os.mkdir('./' + data_folder)
    except FileExistsError as _:
        # OK, the folder exists 
        pass
print("Data folder: %s" % data_folder)

# Check whether the node has an id. If it doesn't, generate one and save it to disk.
try:
    with open(data_folder + '/.id', "r") as id_file:
        node_id = id_file.read()
        print("ID read from file: %s" % node_id)

except FileNotFoundError:
    # This is OK, this must be the first time the node was started
    node_id = random_string(8)
    # Save it to file for the next start
    with open(data_folder + '/.id', "w") as id_file:
        id_file.write(node_id)
        print("New ID generated and saved to file: %s" % node_id)

if is_raspberry_pi():
    # On the Raspberry Pi: ask the user to input the last segment of the server IP address
    server_address = "101" #input("Server address: 192.168.0.___ ")
    push_address = "tcp://192.168.0." + server_address + ":5558"
    subscriber_address = "tcp://192.168.0." + server_address + ":5559"
else:
    # On the local computer: use localhost
    push_address = "tcp://localhost:5558"
    subscriber_address = "tcp://localhost:5559"

context = zmq.Context()

# Socket to send results to the controller
sender = context.socket(zmq.PUSH)
sender.connect(push_address)
# Socket to receive Get Chunk messages from the controller
subscriber = context.socket(zmq.SUB)
subscriber.connect(subscriber_address)
# Receive every message (empty subscription)
subscriber.setsockopt(zmq.SUBSCRIBE, b'')

# HDFS sockets:
hdfs_receive_socket = context.socket(zmq.REP)
hdfs_receive_socket.bind("tcp://*:5560")

hdfs_data_req_socket = context.socket(zmq.REP)
hdfs_data_req_socket.bind("tcp://*:5561")

# Use a Poller to monitor three sockets at the same time
poller = zmq.Poller()
poller.register(subscriber, zmq.POLLIN)
poller.register(hdfs_receive_socket, zmq.POLLIN)
poller.register(hdfs_data_req_socket, zmq.POLLIN)

while True:
    try:
        # Poll all sockets
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break
    pass

    # At this point one or multiple sockets may have received a message

    if hdfs_receive_socket in socks:
        # Incoming message on the 'receiver' socket where we get tasks to store a chunk
        msg = hdfs_receive_socket.recv_multipart()
        # Parse the Protobuf message from the first frame
        task = messages_pb2.storedata_request()
        task.ParseFromString(msg[0])

        # The data is the second frame
        data = msg[1]

        filename = task.filename
        replica_locations_left = task.replica_locations
        print(f'replica_locations_left: {replica_locations_left}')

        print('File to save: %s, size: %d bytes' % (filename, len(data)))

        # Store the chunk with the given filename
        chunk_local_path = data_folder + '/' + filename
        write_file(data, chunk_local_path)
        print("File saved to %s" % chunk_local_path)

        # Send response (just the file name)
        hdfs_receive_socket.send_string(filename)

        if len(replica_locations_left) > 0:
            next_node = replica_locations_left.pop(0)
            delegated_send_socket = context.socket(zmq.REQ)
            delegated_send_socket.connect('tcp://' + next_node + ':5560')

            next_node_task = messages_pb2.storedata_request()
            next_node_task.filename = filename
            next_node_task.replica_locations[:] = replica_locations_left

            delegated_send_socket.send_multipart([
                next_node_task.SerializeToString(),
                data
            ])
            delegated_send_socket.close()

    if hdfs_data_req_socket in socks:
        # Data request for files using hdfs method
        msg = hdfs_data_req_socket.recv()

        # Parse the Protobuf message from the first frame
        task = messages_pb2.getdata_request()
        task.ParseFromString(msg)

        filename = task.filename
        print("File request: %s" % filename)

        # Try to load the requested file from the local file system,
        # send response only if found
        try:
            with open(data_folder + '/' + filename, "rb") as in_file:
                print("Found file %s, sending it back" % filename)

                hdfs_data_req_socket.send_multipart([
                    bytes(filename, 'utf-8'),
                    in_file.read()
                ])
        except FileNotFoundError:
            # This is OK here
            pass

    if subscriber in socks:
        # Incoming message on the 'subscriber' socket where we get retrieve requests
        msg = subscriber.recv()

        # Parse the Protobuf message from the first frame
        task = messages_pb2.getdata_request()
        task.ParseFromString(msg)

        filename = task.filename
        print("Data chunk request: %s" % filename)

        # Try to load the requested file from the local file system,
        # send response only if found
        try:
            with open(data_folder + '/' + filename, "rb") as in_file:
                print("Found chunk %s, sending it back" % filename)

                sender.send_multipart([
                    bytes(filename, 'utf-8'),
                    in_file.read()
                ])
        except FileNotFoundError:
            # This is OK here
            pass

    else:
        print("Message type not supported")
#
