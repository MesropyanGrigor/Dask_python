from solution import Reader
from RandomWordGenerator import RandomWord


"""
dask-worker tcp://192.168.6.40:8786 --nprocs 3 --memory-limit 3GB
"""

rw = RandomWord( max_word_size=10,
                constant_word_size=True,
                include_digits=False,
                special_chars="", #r"@_!#$%^&*()<>?/\|}{~:",
                include_special_chars=False)
rw_b = RandomWord( max_word_size=15,
                  constant_word_size=True,
                  include_digits=True,
                  special_chars=r"@_!#$%^&*()<>?/\|}{~:",
                  include_special_chars=True)

import csv
import random

size = 1000
o_ids = list(range(size+1))

def write_order():
    global o_ids, size
    c_ids = [rw.generate() for i in range(size)]
    with open('order_test.csv', 'w', newline='') as ff:
        writer = csv.writer(ff)
        writer.writerow(['order_id',	'customer_id'])
        writer.writerows([[a1, a2] for a1, a2 in zip(o_ids, c_ids)])

def write_barcode():
    global o_ids, size
    with open('barcode_test.csv', 'w', newline='') as ff:
        writer = csv.writer(ff)
        writer.writerow(['order_id', 'barcode'])
        writer.writerows([[random.randint(0, 1000), rw_b.generate()] for _ in range(size*size*8)])

if __name__ == "__main__":
    if False:
        write_order()
        print("=============")
        write_barcode()
    else:
        csv_reader = Reader('order_test.csv', 'barcode_test.csv', client=True)
        #csv_reader.validate()
        #csv_reader.output()
        #csv_reader.count_unused_barcodes()
        csv_reader.top5()