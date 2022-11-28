import kodo
import math
import random
import copy # for deepcopy
from utils import random_string
import messages_pb2
import json

STORAGE_NODES_NUM = 4

RS_CAUCHY_COEFFS = [
    bytearray([253, 126, 255, 127]),
    bytearray([126, 253, 127, 255]),
    bytearray([255, 127, 253, 126]),
    bytearray([127, 255, 126, 253])
]

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

    # Make sure we can realize max_erasures with 4 storage nodes
    assert(max_erasures >= 0)
    assert(max_erasures < STORAGE_NODES_NUM)

    # How many coded fragments (=symbols) will be required to reconstruct the encoded data. 
    symbols = STORAGE_NODES_NUM - max_erasures
    # The size of one coded fragment (total size/number of symbols, rounded up)
    symbol_size = math.ceil(len(file_data)/symbols)
    # Kodo RLNC encoder using 2^8 finite field
    encoder = kodo.block.Encoder(kodo.FiniteField.binary8)
    encoder.configure(symbols, symbol_size)
    encoder.set_symbols_storage(file_data)
    symbol = bytearray(encoder.symbol_bytes)

    fragment_names = []

    # Generate one coded fragment for each Storage Node
    for i in range(STORAGE_NODES_NUM):
        # Select the next Reed Solomon coefficient vector 
        coefficients = RS_CAUCHY_COEFFS[i]
        # Generate a coded fragment with these coefficients 
        # (trim the coeffs to the actual length we need)
        encoder.encode_symbol(symbol, coefficients[:symbols])
        # Generate a random name for it and save
        name = random_string(8)
        fragment_names.append(name)
        
        # Send a Protobuf STORE DATA request to the Storage Nodes
        task = messages_pb2.storedata_request()
        task.filename = name

        send_task_socket.send_multipart([
            task.SerializeToString(),
            coefficients[:symbols] + bytearray(symbol)
        ])
    
    # Wait until we receive a response for every fragment
    for task_nbr in range(STORAGE_NODES_NUM):
        resp = response_socket.recv_string()
        print('Received: %s' % resp)

    return fragment_names
#


def decode_file(symbols):
    """
    Decode a file using Reed Solomon decoder and the provided coded symbols.
    The number of symbols must be the same as STORAGE_NODES_NUM - max_erasures.

    :param symbols: coded symbols that contain both the coefficients and symbol data
    :return: the decoded file data
    """

    # Reconstruct the original data with a decoder
    symbols_num = len(symbols)
    symbol_size = len(symbols[0]['data']) - symbols_num #subtract the coefficients' size
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
    assert(decoder.is_complete())
    print("File decoded successfully")

    return data_out
#


def get_file(coded_fragments, max_erasures, file_size,
             data_req_socket, response_socket):
    """
    Implements retrieving a file that is stored with Reed Solomon erasure coding

    :param coded_fragments: Names of the coded fragments
    :param max_erasures: Max erasures setting that was used when storing the file
    :param file_size: The original data size. 
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: A list of the random generated chunk names, e.g. (c1,c2), (c3,c4)
    """
    
    # We need 4-max_erasures fragments to reconstruct the file, select this many 
    # by randomly removing 'max_erasures' elements from the given chunk names. 
    fragnames = copy.deepcopy(coded_fragments)
    for i in range(max_erasures):
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

    #Reconstruct the original file data
    file_data = decode_file(symbols)

    return file_data[:file_size]
#


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

    #Reconstruct the original file data
    file_data = decode_file(symbols)

    return file_data[:file_size]# Reconstruct the original data with a decoder
#


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

    #Check that each file is actually stored on the storage nodes
    for file in files:
        print("Checking file with id: %s" % file["id"])
        #We parse the JSON into a python dictionary
        storage_details = json.loads(file["storage_details"])

        #Iterate over each coded fragment to check that it is not missing
        nodes = set() # list of all storage nodes
        nodes_with_fragment = set() # list of storage nodes with fragments
        coded_fragments = storage_details["coded_fragments"] # list of all coded fragments
        missing_fragments = [] # list of missing coded fragments
        existing_fragments = [] # list of existing coded fragments
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
                
                nodes.add(response.node_id) #Build a set of nodes
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
            file_data = get_file_for_repair(existing_fragments[:symbols], # only as many as necessary
                                            file["size"],
                                            repair_socket,
                                            repair_response_socket
            )

            #Build the encoder
            # How many coded fragments (=symbols) will be required to reconstruct the encoded data. 
            symbols = STORAGE_NODES_NUM - storage_details["max_erasures"]
            # The size of one coded fragment (total size/number of symbols, rounded up)
            symbol_size = math.ceil(len(file_data)/symbols)
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

                #Use the node_id as the topic
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
