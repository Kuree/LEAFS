import struct
from itertools import chain

SENSOR_VALUE = 00
DATA_POINTS = 1
IEEE_FLOAT = 00
IEEE_DOUBLE = 1
UNSIGNED_INTEGER = 2

def encode(encoding_type, data, timestamp_type = IEEE_DOUBLE, value_type = IEEE_DOUBLE):
    count = len(data)
    format = _get_encode_codec_string(encoding_type, timestamp_type, value_type, count)
    if encoding_type == DATA_POINTS:
        data = list(chain.from_iterable(data))
    return struct.pack(format, _get_header_encode(encoding_type, timestamp_type, value_type, count), *data)

def decode(msg):
    raw_header_type = msg[0]
    count = msg[1:4]
    encoding_type, timestamp_type, value_type, count = _get_header_decode(raw_header_type, count)
    raw_data = msg[1:] if encoding_type == SENSOR_VALUE else msg[4:]
    format = _get_decode_codec_string(encoding_type, timestamp_type, value_type, count)
    return struct.unpack(format, raw_data)


def _get_header_encode(encoding_type, timestamp_type, value_type, count = 0):
    type_header = encoding_type << 6 | timestamp_type << 2 | value_type
    if encoding_type == SENSOR_VALUE:
        return bytes([type_header])
    elif encoding_type == DATA_POINTS:
        return type_header << 24 | count

def _get_header_decode(raw_header, raw_count = None):
    encoding_type = raw_header >> 6
    timestamp_type = (raw_header >> 2) & 0xFF
    value_type = raw_header & 0xFF
    count = 0
    if encoding_type == DATA_POINTS and count is not None:
        i_1 = raw_count[0]
        i_2 = raw_count[1]
        i_3 = raw_count[2]
        count = i_1 << 16 | i_2 << 8 | i_3
    return encoding_type, timestamp_type, value_type, count

def _get_encode_codec_string(*header):
    if header[0] == SENSOR_VALUE:
        return ">c{0}I{1}".format(_get_type_string(header[1]), _get_type_string(header[2]))
    elif header[0] == DATA_POINTS:
        count = header[-1]
        return ">I{0}".format("{0}{1}".format(_get_type_string(header[1]), _get_type_string(header[2]))*count)

def _get_decode_codec_string(*header):
    if header[0] == SENSOR_VALUE:
        return ">" + "{0}I{1}".format(_get_type_string(header[1]), _get_type_string(header[2]))
    elif header[0] == DATA_POINTS:
        count = header[-1]
        return ">" + "{0}{1}".format(_get_type_string(header[1]), _get_type_string(header[2]))*count
        
    

def _get_type_string(type):
    if type == IEEE_FLOAT:
        return "f"
    elif type == UNSIGNED_INTEGER:
        return "I"
    else:
        return "d"


if __name__ == "__main__":
    import time, msgpack, timeit, cbor, os
    dummy_data = [(time.time(), i) for i in range(100000)]
    iter = 1
    start = time.time()
    f = open("test", 'bw')
    for i in range(iter):
        encode_result = encode(DATA_POINTS, dummy_data, value_type=UNSIGNED_INTEGER)
    end = time.time()
    print("encode time: ", end - start)
    
    f.close()

    start = time.time()
    f = open("test", 'bw')
    for i in range(iter):
        cbor.cbor.dump(dummy_data, f)
    end = time.time()
    print("encode time: ", end - start)
    
    f.close()
    print("ratio" , len(encode_result) / os.stat("test").st_size)