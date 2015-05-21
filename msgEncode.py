import struct
from itertools import chain


class msgEncode:
    # note that to support fusing multiple time series
    # the count is only defined for one time series. Therefore, one reserved field can specify the flag, which
    # will append a count indicating how many time series we have.

    HAS_LIST = 1 << 7
    HAS_COMPUTE = 1 << 6
    HAS_MULTIPLE_SERIES = 1 << 5
    PACK_TUPLE = 1 << 4

    IEEE_FLOAT = 0
    IEEE_DOUBLE = 1
    UNSIGNED_INTEGER = 2
    STRING = 3

    @staticmethod
    def encode(data, timestamp_type = IEEE_DOUBLE, value_type = IEEE_DOUBLE, compute = None):
        has_list = True
        count = len(data)
        if len(data) == 3:
            if isinstance(data[0], int):
                count = 1
                has_list = False
            

        pack_tuple = False
        format = msgEncode._get_encode_codec_string(has_list, compute, timestamp_type, value_type, count)
        if has_list:
            # flatten the list
            # only in python implementation
            # otherwise it's very slow to rearrange the packing
            # in C/C++ it will use tuple style, which will set the PACK_TUPLE to true
            data = list(chain.from_iterable(data))
        header_data = msgEncode._get_header_encode(has_list, pack_tuple, compute, False, timestamp_type, value_type, count)
        if compute is None:
            return struct.pack(format, header_data, *data)
        else:
            raw_compute = list(chain.from_iterable(compute))
            data = raw_compute + data
            return struct.pack(format, header_data, bytes([len(compute)]),  *data)

    @staticmethod
    def decode(msg):
        '''
        This will return a dictionary containing all the information
        '''
        raw_header_type = msg[0:4]
        # has_list, is_pack_tuple, compute, is_multiple_series, timestamp_type, value_type, count = 0
        has_list, is_tuple_pack, has_compute, is_multiple_series, timestamp_type, value_type, count = msgEncode._get_header_decode(raw_header_type)
        # multiple series is not currently supported
        compute_count = 0
        if has_list:
            if has_compute:
                compute_count = int(msg[4])
                raw_data = msg[5 + 8 * compute_count:]
            else:
                raw_data = msg[4:]
        else:
            # for single data point, compute is not allowed
            raw_data = msg[1:]

        # don't need to decode compute here for simplicity
        format = msgEncode._get_decode_codec_string(has_list, is_tuple_pack, is_multiple_series, timestamp_type, value_type, count)
        data_points = struct.unpack(format, raw_data)

        if has_compute:
            compute = msgEncode._get_compute_Data(compute_count, msg[5 : 5 + 8 * compute_count])
            return [data_points, compute]
        else:
            return data_points
    
    @staticmethod
    def _get_compute_Data(count, data):
        result = struct.unpack(">If" * count, data)
        def get_chunks(lst):
            for i in range(len(lst) // 2):
                yield lst[i: i + 2]
        return list(get_chunks(result))

    @staticmethod
    def _get_header_encode(has_list, is_pack_tuple, compute, is_multiple_series, timestamp_type, value_type, count = 0):
        # has_list, pack_tuple, compute, False, timestamp_type, value_type, count
        type_header = 0
        if has_list: type_header |= msgEncode.HAS_LIST
        if is_pack_tuple: type_header |= msgEncode.PACK_TUPLE
        if compute is not None: type_header |= msgEncode.HAS_COMPUTE
        if is_multiple_series: type_header |= msgEncode.HAS_MULTIPLE_SERIES
              
        type_header |= timestamp_type << 2
        type_header |= value_type

        if has_list:
            return type_header << 24 | count
        else:
            return bytes([type_header])

    @staticmethod
    def _get_header_decode(raw_header):
        # has_list, is_pack_tuple, compute, is_multiple_series, timestamp_type, value_type, count = 0
        header_format = raw_header[0]
        has_list = (header_format & msgEncode.HAS_LIST) == msgEncode.HAS_LIST
        is_pack_tuple = (header_format & msgEncode.PACK_TUPLE) == msgEncode.PACK_TUPLE
        has_compute = (header_format & msgEncode.HAS_COMPUTE) == msgEncode.HAS_COMPUTE
        is_multiple_series = (header_format & msgEncode.HAS_MULTIPLE_SERIES) == msgEncode.HAS_MULTIPLE_SERIES
        timestamp_type = (header_format & 12) >> 2
        value_type = header_format & 3
        raw_count = raw_header[1:]
        if has_list:
            i_1 = raw_count[0]
            i_2 = raw_count[1]
            i_3 = raw_count[2]
            count = i_1 << 16 | i_2 << 8 | i_3
        else:
            count = 0
        return has_list, is_pack_tuple, has_compute, is_multiple_series, timestamp_type, value_type, count

    @staticmethod
    def _get_encode_codec_string(has_list, compute, timestamp_type, value_type, count):
        if not has_list:
            return ">c{0}I{1}{2}".format(msgEncode._get_type_string(timestamp_type),msgEncode._get_type_string(value_type), msgEncode._get_compute_header_string(compute))
        else:
            return ">I{0}{1}".format(msgEncode._get_compute_header_string(compute), "{0}I{1}".format(msgEncode._get_type_string(timestamp_type), msgEncode._get_type_string(value_type))*count)

    @staticmethod
    def _get_compute_header_string(compute):
        if compute is None:
            return ""
        else:
            # one byte for compute unit count
            # each compute is defined as integer : type, float, interval
            return "c{0}".format("If" * len(compute))

    @staticmethod
    def _get_decode_codec_string(has_list, is_tuple_pack, is_multiple_series, timestamp_type, value_type, count):
        # multiple series is not supported now
        # assume tuple pack is true
        if has_list:
            return ">" + "{0}I{1}".format(msgEncode._get_type_string(timestamp_type), msgEncode._get_type_string(timestamp_type))*count  
        else:
            return ">" + "{0}I{1}".format(msgEncode._get_type_string(timestamp_type), msgEncode._get_type_string(timestamp_type))
        
    @staticmethod
    def _get_type_string(type):
        if type == msgEncode.IEEE_FLOAT:
            return "f"
        elif type == msgEncode.UNSIGNED_INTEGER:
            return "I"
        elif type == msgEncode.STRING:
            return "s"
        else:
            return "d"

if __name__ == "__main__":
    print(msgEncode.decode(msgEncode.encode([(123, 1, 1), (123, 1, 1), (123, 1, 1)], compute=[(1, 2)])))