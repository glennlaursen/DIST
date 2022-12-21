"""
Aarhus University - Distributed Storage course - Lab 7

Storage Node
"""
import os
import socket
import sys
import logging
import zmq

import messages_pb2
from utils import random_string, write_file, is_raspberry_pi

def find_and_send_file(recv_socker: zmq.Socket, response_socket: zmq.Socket):
    # Data request for files using hdfs method
    msg = recv_socker.recv()

    # Parse the Protobuf message from the first frame
    task = messages_pb2.getdata_request()
    task.ParseFromString(msg)

    filename = task.filename
    print("File request: %s" % filename)

    # Try to load the requested file from the local file system,
    # send response only if found
    try:
        with open(data_folder + '/' + filename, "rb") as in_file:
            print("Found chunk %s, sending it back" % filename)

            response_socket.send_multipart([
                bytes(filename, 'utf-8'),
                in_file.read()
            ])
    except FileNotFoundError:
        # This is OK here
        pass

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
    pull_address = "tcp://192.168.0." + server_address + ":5557"
    push_address = "tcp://192.168.0." + server_address + ":5558"
    subscriber_address = "tcp://192.168.0." + server_address + ":5559"
else:
    # On the local computer: use localhost
    pull_address = "tcp://localhost:5557"
    push_address = "tcp://localhost:5558"
    subscriber_address = "tcp://localhost:5559"

# https://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
own_ip = (([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")] or [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) + ["no IP found"])[0]

context = zmq.Context()

# Socket to receive Store file messages from the controller
raid1_receive_socket = context.socket(zmq.PULL)
raid1_receive_socket.connect(pull_address)
print("Listening on " + pull_address)

# Socket to send results to the controller
sender = context.socket(zmq.PUSH)
sender.connect(push_address)

# Socket to receive Get Chunk messages from the controller
raid1_data_req_socket = context.socket(zmq.SUB)
raid1_data_req_socket.connect(subscriber_address)
# Receive every message (empty subscription)
raid1_data_req_socket.setsockopt(zmq.SUBSCRIBE, b'')

# HDFS sockets:
hdfs_receive_socket = context.socket(zmq.REP)
hdfs_receive_socket.bind("tcp://*:5560")

hdfs_data_req_socket = context.socket(zmq.REP)
hdfs_data_req_socket.bind("tcp://*:5561")

# Status socket:
status_socket = context.socket(zmq.REP)
status_socket.bind("tcp://*:6666")

# Use a Poller to monitor three sockets at the same time
poller = zmq.Poller()
poller.register(raid1_receive_socket, zmq.POLLIN)
poller.register(raid1_data_req_socket, zmq.POLLIN)
poller.register(hdfs_receive_socket, zmq.POLLIN)
poller.register(hdfs_data_req_socket, zmq.POLLIN)
poller.register(status_socket, zmq.POLLIN)

while True:
    try:
        # Poll all sockets
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break
    pass

    # At this point one or multiple sockets may have received a message

    if hdfs_receive_socket in socks:
        # Incoming message on the 'receiver' socket where we get tasks to store a file
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
        find_and_send_file(hdfs_data_req_socket, hdfs_data_req_socket)

    if raid1_receive_socket in socks:
        # Incoming message on the 'receiver' socket where we get tasks to store a file
        msg = raid1_receive_socket.recv_multipart()
        # Parse the Protobuf message from the first frame
        task = messages_pb2.storedata_request()
        task.ParseFromString(msg[0])

        # The data is the second frame
        data = msg[1]

        filename = task.filename
        print('File to save: %s, size: %d bytes' % (filename, len(data)))

        # Store the chunk with the given filename
        chunk_local_path = data_folder + '/' + filename
        write_file(data, chunk_local_path)
        print("File saved to %s" % chunk_local_path)
        # Send response (filename + ip)
        sender.send_pyobj({'filename': task.filename, 'ip': own_ip})

    if raid1_data_req_socket in socks:
        find_and_send_file(raid1_data_req_socket, sender)

    if status_socket in socks:
        status_socket.send_string("OK")

    else:
        print("Message type not supported")
