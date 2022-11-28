"""
Aarhus University - Distributed Storage course - Lab 4

Storage Node
"""
import zmq
import messages_pb2

import sys
import os
import random
import string

from utils import random_string, write_file, is_raspberry_pi

MAX_CHUNKS_PER_FILE = 10

# Read the folder name where chunks should be stored from the first program argument
# (or use the current folder if none was given)
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    # Try to create the folder  
    try:
        os.mkdir('./'+data_folder)
    except FileExistsError as _:
        # OK, the folder exists 
        pass
print("Data folder: %s" % data_folder)

# Check whether the node has an id. If it doesn't, generate one and save it to disk.
try:
    with open(data_folder+'/.id', "r") as id_file:
        node_id = id_file.read()
        print("ID read from file: %s" % node_id)

except FileNotFoundError:
    # This is OK, this must be the first time the node was started
    node_id = random_string(8)
    # Save it to file for the next start
    with open(data_folder+'/.id', "w") as id_file:
        id_file.write(node_id)
        print("New ID generated and saved to file: %s" % node_id)

if is_raspberry_pi():
    # On the Raspberry Pi: ask the user to input the last segment of the server IP address
    server_address = input("Server address: 192.168.0.___ ")
    pull_address = "tcp://192.168.0."+server_address+":5557"
    sender_address = "tcp://192.168.0."+server_address+":5558"
    subscriber_address = "tcp://192.168.0."+server_address+":5559"
    repair_subscriber_address = "tcp://192.168.0."+server_address+":5560"
    repair_sender_address = "tcp://192.168.0."+server_address+":5561"
else:
    # On the local computer: use localhost
    pull_address = "tcp://localhost:5557"
    push_address = "tcp://localhost:5558"
    subscriber_address = "tcp://localhost:5559"
    repair_subscriber_address = "tcp://localhost:5560"
    repair_sender_address = "tcp://localhost:5561"
    

context = zmq.Context()
# Socket to receive Store Chunk messages from the controller
receiver = context.socket(zmq.PULL)
receiver.connect(pull_address)
print("Listening on "+ pull_address)
# Socket to send results to the controller
sender = context.socket(zmq.PUSH)
sender.connect(push_address)
# Socket to receive Get Chunk messages from the controller
subscriber = context.socket(zmq.SUB)
subscriber.connect(subscriber_address)
# Receive every message (empty subscription)
subscriber.setsockopt(zmq.SUBSCRIBE, b'')

# Socket to receive Repair request messages from the controller
repair_subscriber = context.socket(zmq.SUB)
repair_subscriber.connect(repair_subscriber_address)
# Receive messages destined for all nodes
repair_subscriber.setsockopt(zmq.SUBSCRIBE, b'all_nodes')
# Receive messages destined for this node
repair_subscriber.setsockopt(zmq.SUBSCRIBE, node_id.encode('UTF-8'))
# Socket to send repair results to the controller
repair_sender = context.socket(zmq.PUSH)
repair_sender.connect(repair_sender_address)


# Use a Poller to monitor three sockets at the same time
poller = zmq.Poller()
poller.register(receiver, zmq.POLLIN)
poller.register(subscriber, zmq.POLLIN)
poller.register(repair_subscriber, zmq.POLLIN)


while True:
    try:
        # Poll all sockets
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break
    pass

    # At this point one or multiple sockets may have received a message

    if receiver in socks:
        # Incoming message on the 'receiver' socket where we get tasks to store a chunk
        msg = receiver.recv_multipart()
        # Parse the Protobuf message from the first frame
        task = messages_pb2.storedata_request()
        task.ParseFromString(msg[0])

        # The data is the second frame
        data = msg[1]

        print('Chunk to save: %s, size: %d bytes' % (task.filename, len(data)))

        # Store the chunk with the given filename
        chunk_local_path = data_folder+'/'+task.filename
        write_file(data, chunk_local_path)
        print("Chunk saved to %s" % chunk_local_path)

        # Send response (just the file name)
        sender.send_string(task.filename)
        

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
            with open(data_folder+'/'+filename, "rb") as in_file:
                print("Found chunk %s, sending it back" % filename)

                sender.send_multipart([
                    bytes(filename, 'utf-8'),
                    in_file.read()
                ])
        except FileNotFoundError:
            # This is OK here
            pass

    if repair_subscriber in socks:
        # Incoming message on the 'repair_subscriber' socket

        # Parse the multi-part message
        msg = repair_subscriber.recv_multipart()

        # The topic is sent a frame 0
        #topic = str(msg[0])
        
        # Parse the header from frame 1. This is used to distinguish between
        # different types of requests
        header = messages_pb2.header()
        header.ParseFromString(msg[1])

        # Parse the actual message based on the header
        if header.request_type == messages_pb2.FRAGMENT_STATUS_REQ:
            # Fragment Status requests
            task = messages_pb2.fragment_status_request()
            task.ParseFromString(msg[2])

            fragment_name = task.fragment_name
            # Check whether the fragment is on the disk
            fragment_found = os.path.exists(data_folder+'/'+fragment_name) and \
                             os.path.isfile(data_folder+'/'+fragment_name)
            
            if fragment_found == True:
                print("Status request for fragment: %s - Found" % fragment_name)
            else:
                print("Status request for fragment: %s - Not found" % fragment_name)

            # Send the response
            response = messages_pb2.fragment_status_response()
            response.fragment_name = fragment_name
            response.is_present = fragment_found
            response.node_id = node_id

            repair_sender.send(response.SerializeToString())

        elif header.request_type == messages_pb2.FRAGMENT_DATA_REQ:
            # Fragment data request - same implementation as serving normal data
            # requests, except for the different socket the response is sent on
            task = messages_pb2.getdata_request()
            task.ParseFromString(msg[2])

            filename = task.filename
            print("Data chunk request: %s" % filename)

            # Try to load the requested file from the local file system,
            # send response only if found
            try:
                with open(data_folder+'/'+filename, "rb") as in_file:
                    print("Found chunk %s, sending it back" % filename)
                    
                    repair_sender.send_multipart([
                        bytes(filename, 'utf-8'),
                        in_file.read()
                    ])
            except FileNotFoundError:
                # This is OK here
                pass

        elif header.request_type == messages_pb2.STORE_FRAGMENT_DATA_REQ:
            #Fragment store request - same implementation as serving normal data
            # requests, except for the different socket the response is sent on
            task = messages_pb2.storedata_request()
            task.ParseFromString(msg[2])

            # The data is the third frame
            data = msg[3]

            print('Chunk to save: %s, size: %d bytes' % (task.filename, len(data)))
            
            # Store the chunk with the given filename
            chunk_local_path = data_folder+'/'+task.filename
            write_file(data, chunk_local_path)
            print("Chunk saved to %s" % chunk_local_path)

            # Send response (just the file name)
            repair_sender.send_string(task.filename)

        else:
            print("Message type not supported")
#
