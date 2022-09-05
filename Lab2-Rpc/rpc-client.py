from tinyrpc import RPCClient
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport

rpc_client = RPCClient(
    JSONRPCProtocol(),
    HttpPostClientTransport('http://localhost')
)

str_server = rpc_client.get_proxy()

# ...

# call a method called 'reverse_string' with a single string argument
result = str_server.reverse_string('Simple is better.')

print("Server answered:", result)
