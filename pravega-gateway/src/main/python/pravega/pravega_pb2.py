# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: pravega/pravega.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='pravega/pravega.proto',
  package='',
  syntax='proto3',
  serialized_options=_b('\n\"io.pravega.example.pravega_gatewayP\001'),
  serialized_pb=_b('\n\x15pravega/pravega.proto\"\xf4\x01\n\rScalingPolicy\x12\x33\n\tscaleType\x18\x01 \x01(\x0e\x32 .ScalingPolicy.ScalingPolicyType\x12\x13\n\x0btarget_rate\x18\x02 \x01(\x05\x12\x14\n\x0cscale_factor\x18\x03 \x01(\x05\x12\x18\n\x10min_num_segments\x18\x04 \x01(\x05\"i\n\x11ScalingPolicyType\x12\x16\n\x12\x46IXED_NUM_SEGMENTS\x10\x00\x12\x1d\n\x19\x42Y_RATE_IN_KBYTES_PER_SEC\x10\x01\x12\x1d\n\x19\x42Y_RATE_IN_EVENTS_PER_SEC\x10\x02\"#\n\x12\x43reateScopeRequest\x12\r\n\x05scope\x18\x01 \x01(\t\"&\n\x13\x43reateScopeResponse\x12\x0f\n\x07\x63reated\x18\x01 \x01(\x08\"\\\n\x13\x43reateStreamRequest\x12\r\n\x05scope\x18\x01 \x01(\t\x12\x0e\n\x06stream\x18\x02 \x01(\t\x12&\n\x0escaling_policy\x18\x03 \x01(\x0b\x32\x0e.ScalingPolicy\"\'\n\x14\x43reateStreamResponse\x12\x0f\n\x07\x63reated\x18\x01 \x01(\x08\"\\\n\x13UpdateStreamRequest\x12\r\n\x05scope\x18\x01 \x01(\t\x12\x0e\n\x06stream\x18\x02 \x01(\t\x12&\n\x0escaling_policy\x18\x03 \x01(\x0b\x32\x0e.ScalingPolicy\"\'\n\x14UpdateStreamResponse\x12\x0f\n\x07updated\x18\x01 \x01(\x08\"F\n\x11ReadEventsRequest\x12\r\n\x05scope\x18\x01 \x01(\t\x12\x0e\n\x06stream\x18\x02 \x01(\t\x12\x12\n\ntimeout_ms\x18\x03 \x01(\x03\"e\n\x12ReadEventsResponse\x12\r\n\x05\x65vent\x18\x01 \x01(\x0c\x12\x10\n\x08position\x18\x02 \x01(\t\x12\x15\n\revent_pointer\x18\x03 \x01(\t\x12\x17\n\x0f\x63heckpoint_name\x18\x04 \x01(\t\"\x80\x01\n\x12WriteEventsRequest\x12\r\n\x05\x65vent\x18\x01 \x01(\x0c\x12\x13\n\x0brouting_key\x18\x02 \x01(\t\x12\r\n\x05scope\x18\x03 \x01(\t\x12\x0e\n\x06stream\x18\x04 \x01(\t\x12\x17\n\x0fuse_transaction\x18\x05 \x01(\x08\x12\x0e\n\x06\x63ommit\x18\x06 \x01(\x08\"\x15\n\x13WriteEventsResponse\"g\n\tStreamCut\x12 \n\x03\x63ut\x18\x01 \x03(\x0b\x32\x13.StreamCut.CutEntry\x12\x0c\n\x04text\x18\x02 \x01(\t\x1a*\n\x08\x43utEntry\x12\x0b\n\x03key\x18\x01 \x01(\x03\x12\r\n\x05value\x18\x02 \x01(\x03:\x02\x38\x01\"5\n\x14GetStreamInfoRequest\x12\r\n\x05scope\x18\x01 \x01(\t\x12\x0e\n\x06stream\x18\x02 \x01(\t\"a\n\x15GetStreamInfoResponse\x12#\n\x0fhead_stream_cut\x18\x01 \x01(\x0b\x32\n.StreamCut\x12#\n\x0ftail_stream_cut\x18\x02 \x01(\x0b\x32\n.StreamCut\"\x7f\n\x16\x42\x61tchReadEventsRequest\x12\r\n\x05scope\x18\x01 \x01(\t\x12\x0e\n\x06stream\x18\x02 \x01(\t\x12#\n\x0f\x66rom_stream_cut\x18\x03 \x01(\x0b\x32\n.StreamCut\x12!\n\rto_stream_cut\x18\x04 \x01(\x0b\x32\n.StreamCut\"L\n\x17\x42\x61tchReadEventsResponse\x12\r\n\x05\x65vent\x18\x01 \x01(\x0c\x12\x12\n\nsegment_id\x18\x02 \x01(\x03\x12\x0e\n\x06offset\x18\x03 \x01(\x03\x32\xcf\x03\n\x0ePravegaGateway\x12:\n\x0b\x43reateScope\x12\x13.CreateScopeRequest\x1a\x14.CreateScopeResponse\"\x00\x12=\n\x0c\x43reateStream\x12\x14.CreateStreamRequest\x1a\x15.CreateStreamResponse\"\x00\x12=\n\x0cUpdateStream\x12\x14.UpdateStreamRequest\x1a\x15.UpdateStreamResponse\"\x00\x12\x39\n\nReadEvents\x12\x12.ReadEventsRequest\x1a\x13.ReadEventsResponse\"\x00\x30\x01\x12<\n\x0bWriteEvents\x12\x13.WriteEventsRequest\x1a\x14.WriteEventsResponse\"\x00(\x01\x12@\n\rGetStreamInfo\x12\x15.GetStreamInfoRequest\x1a\x16.GetStreamInfoResponse\"\x00\x12H\n\x0f\x42\x61tchReadEvents\x12\x17.BatchReadEventsRequest\x1a\x18.BatchReadEventsResponse\"\x00\x30\x01\x42&\n\"io.pravega.example.pravega_gatewayP\x01\x62\x06proto3')
)



_SCALINGPOLICY_SCALINGPOLICYTYPE = _descriptor.EnumDescriptor(
  name='ScalingPolicyType',
  full_name='ScalingPolicy.ScalingPolicyType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='FIXED_NUM_SEGMENTS', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='BY_RATE_IN_KBYTES_PER_SEC', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='BY_RATE_IN_EVENTS_PER_SEC', index=2, number=2,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=165,
  serialized_end=270,
)
_sym_db.RegisterEnumDescriptor(_SCALINGPOLICY_SCALINGPOLICYTYPE)


_SCALINGPOLICY = _descriptor.Descriptor(
  name='ScalingPolicy',
  full_name='ScalingPolicy',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='scaleType', full_name='ScalingPolicy.scaleType', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='target_rate', full_name='ScalingPolicy.target_rate', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='scale_factor', full_name='ScalingPolicy.scale_factor', index=2,
      number=3, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='min_num_segments', full_name='ScalingPolicy.min_num_segments', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _SCALINGPOLICY_SCALINGPOLICYTYPE,
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=26,
  serialized_end=270,
)


_CREATESCOPEREQUEST = _descriptor.Descriptor(
  name='CreateScopeRequest',
  full_name='CreateScopeRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='scope', full_name='CreateScopeRequest.scope', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=272,
  serialized_end=307,
)


_CREATESCOPERESPONSE = _descriptor.Descriptor(
  name='CreateScopeResponse',
  full_name='CreateScopeResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='created', full_name='CreateScopeResponse.created', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=309,
  serialized_end=347,
)


_CREATESTREAMREQUEST = _descriptor.Descriptor(
  name='CreateStreamRequest',
  full_name='CreateStreamRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='scope', full_name='CreateStreamRequest.scope', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='stream', full_name='CreateStreamRequest.stream', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='scaling_policy', full_name='CreateStreamRequest.scaling_policy', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=349,
  serialized_end=441,
)


_CREATESTREAMRESPONSE = _descriptor.Descriptor(
  name='CreateStreamResponse',
  full_name='CreateStreamResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='created', full_name='CreateStreamResponse.created', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=443,
  serialized_end=482,
)


_UPDATESTREAMREQUEST = _descriptor.Descriptor(
  name='UpdateStreamRequest',
  full_name='UpdateStreamRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='scope', full_name='UpdateStreamRequest.scope', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='stream', full_name='UpdateStreamRequest.stream', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='scaling_policy', full_name='UpdateStreamRequest.scaling_policy', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=484,
  serialized_end=576,
)


_UPDATESTREAMRESPONSE = _descriptor.Descriptor(
  name='UpdateStreamResponse',
  full_name='UpdateStreamResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='updated', full_name='UpdateStreamResponse.updated', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=578,
  serialized_end=617,
)


_READEVENTSREQUEST = _descriptor.Descriptor(
  name='ReadEventsRequest',
  full_name='ReadEventsRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='scope', full_name='ReadEventsRequest.scope', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='stream', full_name='ReadEventsRequest.stream', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='timeout_ms', full_name='ReadEventsRequest.timeout_ms', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=619,
  serialized_end=689,
)


_READEVENTSRESPONSE = _descriptor.Descriptor(
  name='ReadEventsResponse',
  full_name='ReadEventsResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='event', full_name='ReadEventsResponse.event', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=_b(""),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='position', full_name='ReadEventsResponse.position', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='event_pointer', full_name='ReadEventsResponse.event_pointer', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='checkpoint_name', full_name='ReadEventsResponse.checkpoint_name', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=691,
  serialized_end=792,
)


_WRITEEVENTSREQUEST = _descriptor.Descriptor(
  name='WriteEventsRequest',
  full_name='WriteEventsRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='event', full_name='WriteEventsRequest.event', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=_b(""),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='routing_key', full_name='WriteEventsRequest.routing_key', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='scope', full_name='WriteEventsRequest.scope', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='stream', full_name='WriteEventsRequest.stream', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='use_transaction', full_name='WriteEventsRequest.use_transaction', index=4,
      number=5, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='commit', full_name='WriteEventsRequest.commit', index=5,
      number=6, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=795,
  serialized_end=923,
)


_WRITEEVENTSRESPONSE = _descriptor.Descriptor(
  name='WriteEventsResponse',
  full_name='WriteEventsResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=925,
  serialized_end=946,
)


_STREAMCUT_CUTENTRY = _descriptor.Descriptor(
  name='CutEntry',
  full_name='StreamCut.CutEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='StreamCut.CutEntry.key', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='value', full_name='StreamCut.CutEntry.value', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=_b('8\001'),
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1009,
  serialized_end=1051,
)

_STREAMCUT = _descriptor.Descriptor(
  name='StreamCut',
  full_name='StreamCut',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='cut', full_name='StreamCut.cut', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='text', full_name='StreamCut.text', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[_STREAMCUT_CUTENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=948,
  serialized_end=1051,
)


_GETSTREAMINFOREQUEST = _descriptor.Descriptor(
  name='GetStreamInfoRequest',
  full_name='GetStreamInfoRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='scope', full_name='GetStreamInfoRequest.scope', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='stream', full_name='GetStreamInfoRequest.stream', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1053,
  serialized_end=1106,
)


_GETSTREAMINFORESPONSE = _descriptor.Descriptor(
  name='GetStreamInfoResponse',
  full_name='GetStreamInfoResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='head_stream_cut', full_name='GetStreamInfoResponse.head_stream_cut', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='tail_stream_cut', full_name='GetStreamInfoResponse.tail_stream_cut', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1108,
  serialized_end=1205,
)


_BATCHREADEVENTSREQUEST = _descriptor.Descriptor(
  name='BatchReadEventsRequest',
  full_name='BatchReadEventsRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='scope', full_name='BatchReadEventsRequest.scope', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='stream', full_name='BatchReadEventsRequest.stream', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='from_stream_cut', full_name='BatchReadEventsRequest.from_stream_cut', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='to_stream_cut', full_name='BatchReadEventsRequest.to_stream_cut', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1207,
  serialized_end=1334,
)


_BATCHREADEVENTSRESPONSE = _descriptor.Descriptor(
  name='BatchReadEventsResponse',
  full_name='BatchReadEventsResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='event', full_name='BatchReadEventsResponse.event', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=_b(""),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='segment_id', full_name='BatchReadEventsResponse.segment_id', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='offset', full_name='BatchReadEventsResponse.offset', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1336,
  serialized_end=1412,
)

_SCALINGPOLICY.fields_by_name['scaleType'].enum_type = _SCALINGPOLICY_SCALINGPOLICYTYPE
_SCALINGPOLICY_SCALINGPOLICYTYPE.containing_type = _SCALINGPOLICY
_CREATESTREAMREQUEST.fields_by_name['scaling_policy'].message_type = _SCALINGPOLICY
_UPDATESTREAMREQUEST.fields_by_name['scaling_policy'].message_type = _SCALINGPOLICY
_STREAMCUT_CUTENTRY.containing_type = _STREAMCUT
_STREAMCUT.fields_by_name['cut'].message_type = _STREAMCUT_CUTENTRY
_GETSTREAMINFORESPONSE.fields_by_name['head_stream_cut'].message_type = _STREAMCUT
_GETSTREAMINFORESPONSE.fields_by_name['tail_stream_cut'].message_type = _STREAMCUT
_BATCHREADEVENTSREQUEST.fields_by_name['from_stream_cut'].message_type = _STREAMCUT
_BATCHREADEVENTSREQUEST.fields_by_name['to_stream_cut'].message_type = _STREAMCUT
DESCRIPTOR.message_types_by_name['ScalingPolicy'] = _SCALINGPOLICY
DESCRIPTOR.message_types_by_name['CreateScopeRequest'] = _CREATESCOPEREQUEST
DESCRIPTOR.message_types_by_name['CreateScopeResponse'] = _CREATESCOPERESPONSE
DESCRIPTOR.message_types_by_name['CreateStreamRequest'] = _CREATESTREAMREQUEST
DESCRIPTOR.message_types_by_name['CreateStreamResponse'] = _CREATESTREAMRESPONSE
DESCRIPTOR.message_types_by_name['UpdateStreamRequest'] = _UPDATESTREAMREQUEST
DESCRIPTOR.message_types_by_name['UpdateStreamResponse'] = _UPDATESTREAMRESPONSE
DESCRIPTOR.message_types_by_name['ReadEventsRequest'] = _READEVENTSREQUEST
DESCRIPTOR.message_types_by_name['ReadEventsResponse'] = _READEVENTSRESPONSE
DESCRIPTOR.message_types_by_name['WriteEventsRequest'] = _WRITEEVENTSREQUEST
DESCRIPTOR.message_types_by_name['WriteEventsResponse'] = _WRITEEVENTSRESPONSE
DESCRIPTOR.message_types_by_name['StreamCut'] = _STREAMCUT
DESCRIPTOR.message_types_by_name['GetStreamInfoRequest'] = _GETSTREAMINFOREQUEST
DESCRIPTOR.message_types_by_name['GetStreamInfoResponse'] = _GETSTREAMINFORESPONSE
DESCRIPTOR.message_types_by_name['BatchReadEventsRequest'] = _BATCHREADEVENTSREQUEST
DESCRIPTOR.message_types_by_name['BatchReadEventsResponse'] = _BATCHREADEVENTSRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

ScalingPolicy = _reflection.GeneratedProtocolMessageType('ScalingPolicy', (_message.Message,), dict(
  DESCRIPTOR = _SCALINGPOLICY,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:ScalingPolicy)
  ))
_sym_db.RegisterMessage(ScalingPolicy)

CreateScopeRequest = _reflection.GeneratedProtocolMessageType('CreateScopeRequest', (_message.Message,), dict(
  DESCRIPTOR = _CREATESCOPEREQUEST,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:CreateScopeRequest)
  ))
_sym_db.RegisterMessage(CreateScopeRequest)

CreateScopeResponse = _reflection.GeneratedProtocolMessageType('CreateScopeResponse', (_message.Message,), dict(
  DESCRIPTOR = _CREATESCOPERESPONSE,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:CreateScopeResponse)
  ))
_sym_db.RegisterMessage(CreateScopeResponse)

CreateStreamRequest = _reflection.GeneratedProtocolMessageType('CreateStreamRequest', (_message.Message,), dict(
  DESCRIPTOR = _CREATESTREAMREQUEST,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:CreateStreamRequest)
  ))
_sym_db.RegisterMessage(CreateStreamRequest)

CreateStreamResponse = _reflection.GeneratedProtocolMessageType('CreateStreamResponse', (_message.Message,), dict(
  DESCRIPTOR = _CREATESTREAMRESPONSE,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:CreateStreamResponse)
  ))
_sym_db.RegisterMessage(CreateStreamResponse)

UpdateStreamRequest = _reflection.GeneratedProtocolMessageType('UpdateStreamRequest', (_message.Message,), dict(
  DESCRIPTOR = _UPDATESTREAMREQUEST,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:UpdateStreamRequest)
  ))
_sym_db.RegisterMessage(UpdateStreamRequest)

UpdateStreamResponse = _reflection.GeneratedProtocolMessageType('UpdateStreamResponse', (_message.Message,), dict(
  DESCRIPTOR = _UPDATESTREAMRESPONSE,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:UpdateStreamResponse)
  ))
_sym_db.RegisterMessage(UpdateStreamResponse)

ReadEventsRequest = _reflection.GeneratedProtocolMessageType('ReadEventsRequest', (_message.Message,), dict(
  DESCRIPTOR = _READEVENTSREQUEST,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:ReadEventsRequest)
  ))
_sym_db.RegisterMessage(ReadEventsRequest)

ReadEventsResponse = _reflection.GeneratedProtocolMessageType('ReadEventsResponse', (_message.Message,), dict(
  DESCRIPTOR = _READEVENTSRESPONSE,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:ReadEventsResponse)
  ))
_sym_db.RegisterMessage(ReadEventsResponse)

WriteEventsRequest = _reflection.GeneratedProtocolMessageType('WriteEventsRequest', (_message.Message,), dict(
  DESCRIPTOR = _WRITEEVENTSREQUEST,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:WriteEventsRequest)
  ))
_sym_db.RegisterMessage(WriteEventsRequest)

WriteEventsResponse = _reflection.GeneratedProtocolMessageType('WriteEventsResponse', (_message.Message,), dict(
  DESCRIPTOR = _WRITEEVENTSRESPONSE,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:WriteEventsResponse)
  ))
_sym_db.RegisterMessage(WriteEventsResponse)

StreamCut = _reflection.GeneratedProtocolMessageType('StreamCut', (_message.Message,), dict(

  CutEntry = _reflection.GeneratedProtocolMessageType('CutEntry', (_message.Message,), dict(
    DESCRIPTOR = _STREAMCUT_CUTENTRY,
    __module__ = 'pravega.pravega_pb2'
    # @@protoc_insertion_point(class_scope:StreamCut.CutEntry)
    ))
  ,
  DESCRIPTOR = _STREAMCUT,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:StreamCut)
  ))
_sym_db.RegisterMessage(StreamCut)
_sym_db.RegisterMessage(StreamCut.CutEntry)

GetStreamInfoRequest = _reflection.GeneratedProtocolMessageType('GetStreamInfoRequest', (_message.Message,), dict(
  DESCRIPTOR = _GETSTREAMINFOREQUEST,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:GetStreamInfoRequest)
  ))
_sym_db.RegisterMessage(GetStreamInfoRequest)

GetStreamInfoResponse = _reflection.GeneratedProtocolMessageType('GetStreamInfoResponse', (_message.Message,), dict(
  DESCRIPTOR = _GETSTREAMINFORESPONSE,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:GetStreamInfoResponse)
  ))
_sym_db.RegisterMessage(GetStreamInfoResponse)

BatchReadEventsRequest = _reflection.GeneratedProtocolMessageType('BatchReadEventsRequest', (_message.Message,), dict(
  DESCRIPTOR = _BATCHREADEVENTSREQUEST,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:BatchReadEventsRequest)
  ))
_sym_db.RegisterMessage(BatchReadEventsRequest)

BatchReadEventsResponse = _reflection.GeneratedProtocolMessageType('BatchReadEventsResponse', (_message.Message,), dict(
  DESCRIPTOR = _BATCHREADEVENTSRESPONSE,
  __module__ = 'pravega.pravega_pb2'
  # @@protoc_insertion_point(class_scope:BatchReadEventsResponse)
  ))
_sym_db.RegisterMessage(BatchReadEventsResponse)


DESCRIPTOR._options = None
_STREAMCUT_CUTENTRY._options = None

_PRAVEGAGATEWAY = _descriptor.ServiceDescriptor(
  name='PravegaGateway',
  full_name='PravegaGateway',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=1415,
  serialized_end=1878,
  methods=[
  _descriptor.MethodDescriptor(
    name='CreateScope',
    full_name='PravegaGateway.CreateScope',
    index=0,
    containing_service=None,
    input_type=_CREATESCOPEREQUEST,
    output_type=_CREATESCOPERESPONSE,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='CreateStream',
    full_name='PravegaGateway.CreateStream',
    index=1,
    containing_service=None,
    input_type=_CREATESTREAMREQUEST,
    output_type=_CREATESTREAMRESPONSE,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='UpdateStream',
    full_name='PravegaGateway.UpdateStream',
    index=2,
    containing_service=None,
    input_type=_UPDATESTREAMREQUEST,
    output_type=_UPDATESTREAMRESPONSE,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='ReadEvents',
    full_name='PravegaGateway.ReadEvents',
    index=3,
    containing_service=None,
    input_type=_READEVENTSREQUEST,
    output_type=_READEVENTSRESPONSE,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='WriteEvents',
    full_name='PravegaGateway.WriteEvents',
    index=4,
    containing_service=None,
    input_type=_WRITEEVENTSREQUEST,
    output_type=_WRITEEVENTSRESPONSE,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='GetStreamInfo',
    full_name='PravegaGateway.GetStreamInfo',
    index=5,
    containing_service=None,
    input_type=_GETSTREAMINFOREQUEST,
    output_type=_GETSTREAMINFORESPONSE,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='BatchReadEvents',
    full_name='PravegaGateway.BatchReadEvents',
    index=6,
    containing_service=None,
    input_type=_BATCHREADEVENTSREQUEST,
    output_type=_BATCHREADEVENTSRESPONSE,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_PRAVEGAGATEWAY)

DESCRIPTOR.services_by_name['PravegaGateway'] = _PRAVEGAGATEWAY

# @@protoc_insertion_point(module_scope)
