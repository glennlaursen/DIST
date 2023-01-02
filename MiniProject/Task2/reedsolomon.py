import time
from time import sleep

import kodo
import math
import random
import copy  # for deepcopy

import zmq

from utils import random_string, check_nodes, create_logger, get_connected_nodes
import messages_pb2
import json

import logging

# Create custom loggers
logger_encoding = create_logger("rs_encoding", "log_rs_en.log")
logger_decoding = create_logger("rs_decoding", "log_rs_de.log")
#logger_storing = create_logger("rs_storing", "log_rs_st.log")

STORAGE_NODES_NUM = 4

RS_CAUCHY_COEFFS = [
    bytearray([253, 126, 255, 127]),
    bytearray([126, 253, 127, 255]),
    bytearray([255, 127, 253, 126]),
    bytearray([127, 255, 126, 253])
]


def encode_file(file_data, max_erasures):
    t1 = time.perf_counter()

    # Make sure we can realize max_erasures with 4 storage nodes
    assert (max_erasures >= 0)
    assert (max_erasures < STORAGE_NODES_NUM)

    # How many coded fragments (=symbols) will be required to reconstruct the encoded data.
    symbols = STORAGE_NODES_NUM - max_erasures
    # The size of one coded fragment (total size/number of symbols, rounded up)
    symbol_size = math.ceil(len(file_data) / symbols)
    # Kodo RLNC encoder using 2^8 finite field
    encoder = kodo.block.Encoder(kodo.FiniteField.binary8)
    encoder.configure(symbols, symbol_size)
    encoder.set_symbols_storage(file_data)
    symbol = bytearray(encoder.symbol_bytes)

    encoded_fragments = []

    # Generate one coded fragment for each Storage Node
    for i in range(STORAGE_NODES_NUM):
        # Select the next Reed Solomon coefficient vector
        coefficients = RS_CAUCHY_COEFFS[i]
        # Generate a coded fragment with these coefficients
        # (trim the coeffs to the actual length we need)
        encoder.encode_symbol(symbol, coefficients[:symbols])

        encoded_fragments.append(coefficients[:symbols] + bytearray(symbol))

    t2 = time.perf_counter()
    duration = t2-t1
    logger_encoding.info(str(len(file_data)) + "," + str(max_erasures) + "," + str(duration))

    return encoded_fragments


def store_file(file_data, max_erasures, send_task_socket, response_socket):
    """
    Store a file using Reed Solomon erasure coding, protecting it against 'max_erasures'
    unavailable storage nodes.
    The erasure coding part codes are the customized version of the 'encode_decode_using_coefficients'
    example of kodo-python, where you can find a detailed description of each step.

    :param file_data: The file contents to be stored as a Python bytearray
    :param max_erasures: How many storage node failures should the data survive
    :param send_task_socket: A ZMQ PUSH socket to the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond
    :return: A list of the coded fragment names, e.g. (c1,c2,c3,c4)
    """

    encoded_fragments = encode_file(file_data,max_erasures)
    fragment_names = [random_string(8) for _ in encoded_fragments]

    # Generate one coded fragment for each Storage Node
    for i, fragment in enumerate(encoded_fragments):

        # Send a Protobuf STORE DATA request to the Storage Nodes
        task = messages_pb2.storedata_request()
        task.filename = fragment_names[i]

        send_task_socket.send_multipart([
            task.SerializeToString(),
            fragment
        ])

    # Wait until we receive a response for every fragment
    for _ in encoded_fragments:
        resp = response_socket.recv_string()
        print('Received: %s' % resp)

    return fragment_names


def store_file_delegate(data, max_erasures, heartbeat_socket, response_socket, context):
    # Delegate storage
    ips = get_connected_nodes(heartbeat_socket, response_socket, STORAGE_NODES_NUM)
    rand_ip = random.choice(ips)
    print("Delegating encoding to", rand_ip)
    ips.remove(rand_ip)

    encode_socket = context.socket(zmq.REQ)
    addr = "tcp://" + rand_ip + ':5542'
    encode_socket.connect(addr)

    encode_socket.send_pyobj({
        "data": data,
        "ips": ips,
        "max_erasures": max_erasures,
        "n_nodes": STORAGE_NODES_NUM
    })

    result = encode_socket.recv_pyobj()
    return result['names']


def decode_file(symbols, max_erasures):
    """
    Decode a file using Reed Solomon decoder and the provided coded symbols.
    The number of symbols must be the same as STORAGE_NODES_NUM - max_erasures.

    :param symbols: coded symbols that contain both the coefficients and symbol data
    :return: the decoded file data
    """
    t1 = time.perf_counter()

    # Reconstruct the original data with a decoder
    symbols_num = len(symbols)
    symbol_size = len(symbols[0]['data']) - symbols_num  # subtract the coefficients' size
    decoder = kodo.block.Decoder(kodo.FiniteField.binary8)
    decoder.configure(symbols_num, symbol_size)
    data_out = bytearray(decoder.block_bytes)
    decoder.set_symbols_storage(data_out)

    for symbol in symbols:
        # Separate the coefficients from the symbol data
        coefficients = symbol['data'][:symbols_num]
        symbol_data = symbol['data'][symbols_num:]
        # Feed it to the decoder
        decoder.decode_symbol(symbol_data, coefficients)

    # Make sure the decoder successfully reconstructed the file
    assert (decoder.is_complete())

    t2 = time.perf_counter()
    duration = t2-t1
    logger_decoding.info(str(len(data_out)) + "," + str(max_erasures) + "," + str(duration))

    print("File decoded successfully")

    return data_out


def get_file(coded_fragments, max_erasures, file_size,
             data_req_socket, heartbeat_req_socket, response_socket):
    """
    Implements retrieving a file that is stored with Reed Solomon erasure coding

    :param coded_fragments: Names of the coded fragments
    :param max_erasures: Max erasures setting that was used when storing the file
    :param file_size: The original data size. 
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: A list of the random generated chunk names, e.g. (c1,c2), (c3,c4)
    """
    nodes_needed = STORAGE_NODES_NUM - max_erasures
    fragnames = copy.deepcopy(coded_fragments)
    #connected_nodes, _ = check_nodes(heartbeat_req_socket, response_socket, STORAGE_NODES_NUM)
    connected_nodes = get_connected_nodes(heartbeat_req_socket, response_socket, STORAGE_NODES_NUM)

    # if > max_erasures nodes are dead
    if len(connected_nodes) < nodes_needed:
        msg = "Not enough nodes online to fetch file"
        print(msg)
        return msg
    # if <= max_erasures nodes are dead
    #elif len(connected_nodes) >= nodes_needed:
        # Get list of all frags located on live nodes
        #all_frags = [item for sublist in connected_nodes.values() for item in sublist]

        # Copy of fragnames to loop over since we have to clear fragnames
        #new_fragnames = copy.deepcopy(fragnames)

        # New list of needed fragments found on live nodes only
        #fragnames.clear()
        #fragnames = [frag for frag in new_fragnames if frag in all_frags]

        # # If there are more fragments than needed, remove the excess
        # if len(fragnames) > nodes_needed:
        #     to_remove = len(fragnames) - nodes_needed
        #     for i in range(to_remove):
        #         fragnames.remove(random.choice(fragnames))

    # Request the coded fragments in parallel
    for name in fragnames:
        task = messages_pb2.getdata_request()
        task.filename = name
        data_req_socket.send(
            task.SerializeToString()
        )

    # Receive all chunks and insert them into the symbols array
    symbols = []
    for _ in range(nodes_needed):
        if response_socket.poll(500, zmq.POLLIN):
            result = response_socket.recv_multipart()
            # In this case we don't care about the received name, just use the
            # data from the second frame
            symbols.append({
                "chunkname": result[0].decode('utf-8'),
                "data": bytearray(result[1])
            })
    print("All coded fragments received successfully")

    print("Symbols:", len(symbols))

    # Reconstruct the original file data
    file_data = decode_file(symbols[:nodes_needed], max_erasures)

    return file_data[:file_size]


def get_file_delegate(coded_fragments, max_erasures, file_size,
             data_req_socket, heartbeat_req_socket, response_socket, context):

    nodes_needed = STORAGE_NODES_NUM - max_erasures
    fragnames = copy.deepcopy(coded_fragments)
    connected_nodes, ips = check_nodes(heartbeat_req_socket, response_socket, STORAGE_NODES_NUM)

    # if > max_erasures nodes are dead
    if len(connected_nodes) < nodes_needed:
        msg = "Not enough nodes online to fetch file"
        print(msg)
        return msg
    # if <= max_erasures nodes are dead
    elif len(connected_nodes) >= nodes_needed:
        # Get list of all frags located on live nodes
        all_frags = [item for sublist in connected_nodes.values() for item in sublist]

        # Copy of fragnames to loop over since we have to clear fragnames
        new_fragnames = copy.deepcopy(fragnames)

        # New list of needed fragments found on live nodes only
        fragnames.clear()
        fragnames = [frag for frag in new_fragnames if frag in all_frags]

        # If there are more fragments than needed, remove the excess
        if len(fragnames) > nodes_needed:
            to_remove = len(fragnames) - nodes_needed
            for i in range(to_remove):
                fragnames.remove(random.choice(fragnames))

    # Request the coded fragments in parallel
    for name in fragnames:
        task = messages_pb2.getdata_request()
        task.filename = name
        data_req_socket.send(
            task.SerializeToString()
        )

    # Receive all chunks and insert them into the symbols array
    symbols = []
    for _ in range(len(fragnames)):
        result = response_socket.recv_multipart()
        # In this case we don't care about the received name, just use the
        # data from the second frame
        symbols.append({
            "chunkname": result[0].decode('utf-8'),
            "data": bytearray(result[1])
        })
    print("All coded fragments received successfully")

    rand_ip = random.choice(ips)
    print("Delegating decoding to", rand_ip)
    ips.remove(rand_ip)

    decode_socket = context.socket(zmq.REQ)
    addr = "tcp://" + rand_ip + ':5543'
    decode_socket.connect(addr)

    decode_socket.send_pyobj({
        "data": symbols,
        "size": file_size,
        "max_erasures": max_erasures
    })

    result = decode_socket.recv()
    file_data = result

    return file_data[:file_size]


def get_file_for_repair(fragments_to_retrieve, file_size,
                        repair_socket, repair_response_socket):
    """
    Implements retrieving a file that is stored with Reed Solomon erasure coding for use
    in the repair process. Apart from the communication with the storage nodes, the
    implementation is similar to how a file is retrieved using get_file.

    :param fragments_to_retrieve: Names of the coded fragments that should be retrieved
    :param file_size: The original data size. 
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: A list of the random generated chunk names, e.g. (c1,c2), (c3,c4)
    """

    # Request the coded fragments in parallel.
    for name in fragments_to_retrieve:
        task = messages_pb2.getdata_request()
        task.filename = name
        header = messages_pb2.header()
        header.request_type = messages_pb2.FRAGMENT_DATA_REQ
        repair_socket.send_multipart([b"all_nodes",
                                      header.SerializeToString(),
                                      task.SerializeToString()])

    # Receive all chunks and insert them into the symbols array
    symbols = []
    for _ in range(len(fragments_to_retrieve)):
        result = repair_response_socket.recv_multipart()
        # In this case we don't care about the received name, just use the 
        # data from the second frame
        symbols.append({
            "chunkname": result[0].decode('utf-8'),
            "data": bytearray(result[1])
        })
    print(str(len(fragments_to_retrieve)) + " coded fragments received successfully")

    # Reconstruct the original file data
    file_data = decode_file(symbols)

    return file_data[:file_size]  # Reconstruct the original data with a decoder


def check_nodes_delete(heartbeat_request_socket, response_socket):
    connected_nodes_fragments = {}
    connected_nodes_ip = []

    task = messages_pb2.heartbeat_request()
    heartbeat_request_socket.send_multipart([b"all_nodes", task.SerializeToString()])

    # Check to see which nodes are alive
    for i in range(STORAGE_NODES_NUM):
        if (response_socket.poll(500) & zmq.POLLIN) != 0:
            msg = response_socket.recv()
            response = messages_pb2.heartbeat_response()
            response.ParseFromString(msg)
            connected_nodes_fragments[response.node_id] = response.fragments
            connected_nodes_ip.append(response.node_ip)
            # print("Node alive:", response.node_id)

    return connected_nodes_fragments, connected_nodes_ip


def start_repair_process(files, repair_socket, repair_response_socket):
    """
    Implements the repair process for Reed Solomon erasure coding. It receives a list
    of files that are to be checked. For each file, it sends queries to the Storage
    nodes to check that all coded fragments are stored safely. If it finds a missing
    fragment, it determines which Storage node was supposed to store it and repairs it.
    This happens by first retrieving the original file data, then re-encoding the missing
    fragment. It also handles multiple missing fragments for a file, as long as their
    number does not exceed `max_erasures`.

    :param files: List of files to be checked
    :param repair_socket: A ZMQ PUB socket to send requests to the storage nodes
    :param repair_response_socket: A ZMQ PULL socket on which the storage nodes respond.
    :return: the number of missing fragments, the number of repaired fragments
    """

    number_of_missing_fragments = 0
    number_of_repaired_fragments = 0

    # Check that each file is actually stored on the storage nodes
    for file in files:
        print("Checking file with id: %s" % file["id"])
        # We parse the JSON into a python dictionary
        storage_details = json.loads(file["storage_details"])

        # Iterate over each coded fragment to check that it is not missing
        nodes = set()  # list of all storage nodes
        nodes_with_fragment = set()  # list of storage nodes with fragments
        coded_fragments = storage_details["coded_fragments"]  # list of all coded fragments
        missing_fragments = []  # list of missing coded fragments
        existing_fragments = []  # list of existing coded fragments
        for fragment in coded_fragments:
            task = messages_pb2.fragment_status_request()
            task.fragment_name = fragment
            header = messages_pb2.header()
            header.request_type = messages_pb2.FRAGMENT_STATUS_REQ

            repair_socket.send_multipart([b"all_nodes",
                                          header.SerializeToString(),
                                          task.SerializeToString()])

            fragment_found = False
            # Wait until we receive a response from each node
            for task_nbr in range(STORAGE_NODES_NUM):
                msg = repair_response_socket.recv()
                response = messages_pb2.fragment_status_response()
                response.ParseFromString(msg)

                nodes.add(response.node_id)  # Build a set of nodes
                if response.is_present == True:
                    nodes_with_fragment.add(response.node_id)
                    existing_fragments.append(fragment)
                    fragment_found = True

            if fragment_found == False:
                print("Fragment %s lost" % fragment)
                missing_fragments.append(fragment)
                number_of_missing_fragments += 1
            else:
                print("Fragment %s OK" % fragment)

        # If we have lost fragments, we must figure out where they were stored
        # We assume that each node has exactly 1 or 0 fragments
        nodes_without_fragment = list(nodes.difference(nodes_with_fragment))

        # Perform the actual repair, if necessary
        if len(missing_fragments) > 0:
            # Check that enough fragments still remain to be able to repair
            if len(missing_fragments) > storage_details["max_erasures"]:
                print("Too many lost fragments: %s. Unable to repair file. " % len(missing_fragments))
                continue

            # Retrieve sufficient fragments and decode
            symbols = STORAGE_NODES_NUM - storage_details["max_erasures"]
            file_data = get_file_for_repair(existing_fragments[:symbols],  # only as many as necessary
                                            file["size"],
                                            repair_socket,
                                            repair_response_socket
                                            )

            # Build the encoder
            # How many coded fragments (=symbols) will be required to reconstruct the encoded data. 
            symbols = STORAGE_NODES_NUM - storage_details["max_erasures"]
            # The size of one coded fragment (total size/number of symbols, rounded up)
            symbol_size = math.ceil(len(file_data) / symbols)
            # Kodo RLNC encoder using 2^8 finite field
            encoder = kodo.block.Encoder(kodo.FiniteField.binary8)
            encoder.configure(symbols, symbol_size)
            encoder.set_symbols_storage(file_data)
            symbol = bytearray(encoder.symbol_bytes)

            # Re-encode each missing fragment: 
            for missing_fragment in missing_fragments:
                fragment_index = coded_fragments.index(missing_fragment)
                # Select the appropriate Reed Solomon coefficient vector
                coefficients = RS_CAUCHY_COEFFS[fragment_index]
                # Generate a coded fragment with these coefficients
                # (trim the coeffs to the actual length we need)
                encoder.encode_symbol(symbol, coefficients[:symbols])

                # Save with the same name as before
                # Send a Protobuf STORE DATA request to the Storage Nodes
                task = messages_pb2.storedata_request()
                task.filename = missing_fragment

                header = messages_pb2.header()
                header.request_type = messages_pb2.STORE_FRAGMENT_DATA_REQ

                node_id = nodes_without_fragment[number_of_repaired_fragments]

                # Use the node_id as the topic
                repair_socket.send_multipart([node_id.encode('UTF-8'),
                                              header.SerializeToString(),
                                              task.SerializeToString(),
                                              coefficients[:symbols] + bytearray(symbol)
                                              ])
                number_of_repaired_fragments += 1

            # Wait until we receive a response for every fragment
            for task_nbr in range(len(missing_fragments)):
                resp = repair_response_socket.recv_string()
                print('Repaired fragment: %s' % resp)

    return number_of_missing_fragments, number_of_repaired_fragments
