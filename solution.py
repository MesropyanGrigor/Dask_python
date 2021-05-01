"""
Read two input csv files and write into stdout, by following way:
    customer_id, order_id1, [barcode1, barcode2, ...]
    customer_id, order_id2, [barcode1, barcode2, ...]

columns(order.csv):  customer_id, order_id
columns(barcodes.csv): order_id, barcode

Validation:
==========

    1. No duplicate barcodes
    2. No orders without barcodes    
  Items which failed the validation should be logged and being ignored in further

Constrains:
==========

    1. This should work with very big input files.
    2. This should be able to hanlde very big files in parallel.
    3. This should provide TOP 5 customers, by following format:
                customer_id, amount_of_tickets
    4. Print the amount of unused barcodes (barcodes left).

Requirements
============
bokeh>=0.13.0
"dask[distrubuted]"
"""

import logging
import argparse

from dask import dataframe as df
from dask.distributed import Client
from os import path


class Parser:
    """
    A Parser class to render some data

    It accept two csv files as input and converted those into dask.dataframe.
    The dask framework is used to handl very big files also for achiving 
    parallel compuing.

    Parameters
    ==========
        input_1: path
            order.csv file path
        input_2: path
            barcodes.csv file path

    Examples
    ========
    >>> csv_reader = Parser('orders.csv', 'barcodes.csv')

    Validating given input files and logging into 'monitor.log' 
    file if there are errors:

    >>> csv_reader.validate()

    >>> csv_reader.output() # print customer, order_id, [barcodes] "TODO update messsage"

    >>> csv_reader.count_unused_barcodes() # Print unused barcodes number

    >>> csv_reader.top5()  # Getting Top 5 customers
    """
    def __init__(self, input_1:'orders.csv', input_2:'barcodes.csv',
                 client: bool=False, file_name:str='monitor.log'):
        self.ddf_1 = df.read_csv(input_1, dtype=str)
        print(f"Partitions number for {input_1}: {self.ddf_1.npartitions}")
        self.ddf_2 = df.read_csv(input_2, dtype=str, blocksize=5e6) # 25MB
        print(f"Partitions number for {input_2}: {self.ddf_2.npartitions}")
        self.__init__logging(file_name)
        # For using distrubuted mechanism I tried by the easy way but it was
        # successfull by given paramters and also I tried in Hard way and
        # it was successfull.
        # Hard way:
        #   First of all need to have dask-scheduler and then dask-worker 
        #   and after that connecting via python into the schedular.
        # 
        #   CMD: dask-scheduler
        #    CMD: dask-worker tcp://192.168.6.40:8786 
        # 
        #   Also for proper work need to update PYTHONPATH provide the 
        #   current python file path if it doesn't see by python
        #
        # It worked in my case under windows system
        if client:
            #client = Client("192.168.6.40:8786")# n_workders=2,  momory_limit='1GB')
            client = Client( threads_per_worker=2, processes=False, n_workers=2)#, momory_limit='2GB')
            print(client)

    def __init__logging(self, file_name:str):
        """Initializing LOGGER"""
        self.log = logging.getLogger(__name__)
        fhandler = logging.FileHandler(file_name, mode='w')
        formatter = logging.Formatter("%(levelname)s: %(message)s")
        fhandler.setFormatter(formatter)
        self.log.handlers = [fhandler] 
        self.log.setLevel(logging.DEBUG)

    def output(self):
        """
        Writing into the stdout Stream cvs files contents by following  format:

            customer_id, order_id1, [barcode1, barcode2, ...]
            customer_id, order_id2, [barcode1, barcode2, ...]

        Currently used dask.groupby method for parallel computing.
        """
        for _, row in self.ddf_1.iterrows():
            customer_id = row['customer_id']
            o_id = row['order_id']
            group = self.ddf_2[self.ddf_2['order_id'] == o_id]
            barcodes = group['barcode'].values.compute()
            print(f"{o_id}, {customer_id}, [{', '.join(barcodes)}]", flush=True)

    def validate(self):
        """
        Validating inputs files, logging into the file and
        drop rows if validation fails:

            1. No duplicate barcodes
            2. No orders without barcodes
        """
        map_df = self.ddf_2.map_partitions(self._get_duplicates)
        self._cmp_and_log("The duplicated rows:", map_df)
        size_before = self.ddf_2.map_partitions(len).compute().sum()
        self.ddf_2 = self.ddf_2.drop_duplicates(keep='first')#.compute()
        droped_lines_n = size_before - self.ddf_2.map_partitions(len).compute().sum()
        if droped_lines_n: 
            print(f"Droped duplicated {droped_lines_n} rows in the datfram")
        map_df = self.ddf_2.map_partitions(self._get_barcode_NaN_values)
        self._cmp_and_log("The following rows' barcode is NaN:", map_df)
        self.ddf_2 = self.ddf_2.dropna(subset=['barcode'])
        
    @staticmethod
    def _get_duplicates(pdf):
        """
        Returning datafram where there are duplicates
        """
        return  pdf[pdf.duplicated()]
        
    @staticmethod
    def _get_barcode_NaN_values(pdf):
        """
        Returning datafram where baracode column is NaN
        """
        return pdf[pdf['barcode'].isna()]#.any(axis=1)]
        
    def _cmp_and_log(self, message, map_df):
        """
        Compute and Log errors into the log file if there are
        """
        df = map_df.compute()
        if not df.empty:
            self.log.error(message)
            self.log.error(df.to_string())

    def count_unused_barcodes(self):
        """
        Counting and Printing unused barcodes number
        """
        print("Unused barcodes count is: "
              f"{len(self.ddf_2[self.ddf_2['order_id'].isna()])}")

    def top5(self):
        """
        Counting and Printing Top 5 customers 
        """
        df = self.ddf_1.merge(self.ddf_2, on='order_id', how='inner')
        df = df['customer_id'].value_counts().head(5)
        print("Top 5 customers")
        for customer_id, count in df.items():
            print(f"{customer_id}, {count}")


def parse_args():
    """argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-input_order', dest='input_order',
                         default=path.join('data', 'orders.csv'),
                        help="Input order csv file path")
    parser.add_argument('-input_barcode', dest='input_barcode',
                        default=path.join('data', 'barcodes.csv'),
                        help="Input barcode csv file path")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args() 
    csv_reader = Parser(args.input_order, args.input_barcode, client=True)
    csv_reader.validate()
    csv_reader.output()
    csv_reader.top5()
    csv_reader.count_unused_barcodes()

    

