import platform
import random
import string

node_ips = ['192.168.38.10' + i for i in ["1", "2", "3", "4"]]
node_names_for_docker = ['node' + i for i in ["1", "2", "3", "4"]]


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


def is_raspberry_pi():
    """
    Returns True if the current platform is a Raspberry Pi, otherwise False.
    """
    return 'raspberrypi' in platform.uname().node


def is_docker():
    return 'WSL' in platform.uname().release


def get_k_node_ips(k: int):
    if is_raspberry_pi():
        return random.sample(node_ips, k)
    elif is_docker():
        return random.sample(node_names_for_docker, k)
    else:
        return random.sample(node_ips, k)
