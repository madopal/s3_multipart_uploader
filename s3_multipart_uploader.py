#!/usr/bin/env python

import os, sys
import boto.s3.connection
from boto.s3.key import Key 
from argparse import ArgumentParser
import md5
import json
import time
from cStringIO import StringIO as BIO

def parse_cmd_args():
    parser = ArgumentParser()
    parser.add_argument("config",
        help="file name to load s3 config from",
    )
    parser.add_argument("bucket_name",
        help="bucket name to upload key to",
    )
    parser.add_argument("file_name",
        help="file name to upload"
    )

    args = parser.parse_args()

    return args

def get_nearest_file_size(size):
    sizes = [
        (1000000000000000, "PB"),
        (1000000000000, "TB"),
        (1000000000, "GB"),
        (1000000, "MB"),
        (1000, "KB") 
    ]

    value = sizes[len(sizes) - 1]
    for entry in sizes:
        if size < entry[0]:
            continue
        value = entry
        break

    return value

def upload_multipart_file(conn, bucket_name, key_name, file_iterator, calc_md5=False, stream_status=True):
    print "Uploading to key %s, bucket %s" % (key_name, bucket_name)
    md5_sum = md5.new()
    bucket_exists = False
    print "Checking buckets"
    bucket_exists = False
    try:
        bucket_list = conn.get_all_buckets()
    except Exception as e:
        print"Unable to list buckets:", e
    else:
        for instance in bucket_list:
            if instance.name == bucket_name:
                bucket_exists = True
                break

    if bucket_exists:
        bucket = conn.get_bucket(bucket_name)
    else:
        bucket = conn.create_bucket(bucket_name)

    print "Creating stream"
    stream_buffer = BIO()
    mp_chunk_size = 1073741824 # 1GiB
    cur_size = 0
    chunk_index = 1
    total_size = 0
    print "Initiating multipart upload"
    mp = bucket.initiate_multipart_upload(key_name)
    start_time = time.clock()

    for chunk in file_iterator:
        size_info = get_nearest_file_size(total_size)
        cur_time = time.clock()
        base_transfer_rate = float(total_size) / float(cur_time - start_time)
        transfer_info = get_nearest_file_size(base_transfer_rate)
        cur_conv_size = float(total_size) / float(size_info[0])
        cur_conv_rate = base_transfer_rate / float(transfer_info[0])
        if stream_status:
            sys.stdout.write("%7.02f %s : %6.02f %s per sec\r" % (
                cur_conv_size, size_info[1],
                cur_conv_rate, transfer_info[1]))
            sys.stdout.flush()
        stream_buffer.write(chunk)
        cur_size += len(chunk)
        total_size += len(chunk)

        if calc_md5:
            md5_sum.update(chunk)

        if cur_size >= mp_chunk_size:
            os.environ['http_proxy'] = "http://cloud-proxy:3128"
            os.environ['https_proxy'] = "http://cloud-proxy:3128"
            stream_buffer.seek(0)
            try:
                result = mp.upload_part_from_file(stream_buffer, chunk_index)
            except Exception as e:
                print "Error writing %d bytes" % cur_size
                print e
                sys.exit()
            else:
                cur_size = 0
                stream_buffer = BIO()
                chunk_index += 1
            del os.environ['https_proxy']
            del os.environ['http_proxy']


    # write the remaining data
    os.environ['http_proxy'] = "http://cloud-proxy:3128"
    os.environ['https_proxy'] = "http://cloud-proxy:3128"
    stream_buffer.seek(0)
    try:
        result = mp.upload_part_from_file(stream_buffer, chunk_index)
    except Exception as e:
        print "Error writing %d bytes" % cur_size
        print e
        sys.exit()
    else:
        cur_size = 0
    del os.environ['https_proxy']
    del os.environ['http_proxy']

    cur_time = time.clock()
    size_info = get_nearest_file_size(total_size)
    base_transfer_rate = float(total_size) / float(cur_time - start_time)
    transfer_info = get_nearest_file_size(base_transfer_rate)
    cur_conv_size = float(total_size) / float(size_info[0])
    cur_conv_rate = base_transfer_rate / float(transfer_info[0])
    print "Complete, %7.02f %s : %6.02f %s per sec\r" % (
        cur_conv_size, size_info[1],
        cur_conv_rate, transfer_info[1]
		)

    if cur_size > 0:
        os.environ['http_proxy'] = "http://cloud-proxy:3128"
        os.environ['https_proxy'] = "http://cloud-proxy:3128"
        stream_buffer.seek(0)
        try:
            result = mp.upload_part_from_file(stream_buffer, chunk_index)
        except Exception as e:
            print "Error writing %d bytes" % cur_size
            print e


    mp.complete_upload()
    print "Upload complete, md5 = %s, %d bytes transferred" % (
        str(md5_sum.hexdigest()), total_size
		)

    return {"md5_sum" : str(md5_sum.hexdigest()), "bytes_transferred" : total_size }


args = parse_cmd_args()

# load json config
with open(args.config, "r") as json_file:
    creds = json.load(json_file)

#is_secure=creds.get('secure'),
conn = boto.connect_s3(
    aws_access_key_id=creds.get('access_key'),
    aws_secret_access_key=creds.get('secret_key'),
    host=creds.get('host'),
    calling_format=boto.s3.connection.OrdinaryCallingFormat(),
    )

with open(args.file_name, "r") as file_data:
    file_info = upload_multipart_file(
        conn, 
        args.bucket_name, 
        args.file_name, 
        file_data,
        True
    )
print file_info

