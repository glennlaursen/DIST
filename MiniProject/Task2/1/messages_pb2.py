# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: messages.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0emessages.proto\"%\n\x11storedata_request\x12\x10\n\x08\x66ilename\x18\x01 \x01(\t\"#\n\x0fgetdata_request\x12\x10\n\x08\x66ilename\x18\x01 \x01(\t\"0\n\x17\x66ragment_status_request\x12\x15\n\rfragment_name\x18\x01 \x01(\t\"V\n\x18\x66ragment_status_response\x12\x15\n\rfragment_name\x18\x01 \x01(\t\x12\x12\n\nis_present\x18\x02 \x01(\x08\x12\x0f\n\x07node_id\x18\x03 \x01(\t\"-\n\x06header\x12#\n\x0crequest_type\x18\x01 \x01(\x0e\x32\r.request_type\"$\n\x11heartbeat_request\x12\x0f\n\x07node_ip\x18\x01 \x01(\t\"8\n\x12heartbeat_response\x12\x0f\n\x07node_id\x18\x01 \x01(\t\x12\x11\n\tfragments\x18\x02 \x03(\t*n\n\x0crequest_type\x12\x17\n\x13\x46RAGMENT_STATUS_REQ\x10\x00\x12\x15\n\x11\x46RAGMENT_DATA_REQ\x10\x01\x12\x1b\n\x17STORE_FRAGMENT_DATA_REQ\x10\x02\x12\x11\n\rHEARTBEAT_REQ\x10\x03\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'messages_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _REQUEST_TYPE._serialized_start=375
  _REQUEST_TYPE._serialized_end=485
  _STOREDATA_REQUEST._serialized_start=18
  _STOREDATA_REQUEST._serialized_end=55
  _GETDATA_REQUEST._serialized_start=57
  _GETDATA_REQUEST._serialized_end=92
  _FRAGMENT_STATUS_REQUEST._serialized_start=94
  _FRAGMENT_STATUS_REQUEST._serialized_end=142
  _FRAGMENT_STATUS_RESPONSE._serialized_start=144
  _FRAGMENT_STATUS_RESPONSE._serialized_end=230
  _HEADER._serialized_start=232
  _HEADER._serialized_end=277
  _HEARTBEAT_REQUEST._serialized_start=279
  _HEARTBEAT_REQUEST._serialized_end=315
  _HEARTBEAT_RESPONSE._serialized_start=317
  _HEARTBEAT_RESPONSE._serialized_end=373
# @@protoc_insertion_point(module_scope)
