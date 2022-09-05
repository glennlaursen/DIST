import base64
import string

import gevent.pywsgi
import gevent.queue
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.server.gevent import RPCServerGreenlets
from tinyrpc.transports.wsgi import WsgiServerTransport

import utils

dispatcher = RPCDispatcher()
transport = WsgiServerTransport(queue_class=gevent.queue.Queue)

# start wsgi server as a background-greenlet
wsgi_server = gevent.pywsgi.WSGIServer(('127.0.0.1', 80), transport.handle)
gevent.spawn(wsgi_server.serve_forever)

rpc_server = RPCServerGreenlets(
    transport,
    JSONRPCProtocol(),
    dispatcher
)


@dispatcher.public
def reverse_string(s):
    return s[::-1]

@dispatcher.public
def store_file(filename: string, contents_b64: string):
    binary_data = base64.b64decode(contents_b64)
    return utils.write_file(binary_data, filename)


# in the main greenlet, run our rpc_server
rpc_server.serve_forever()
