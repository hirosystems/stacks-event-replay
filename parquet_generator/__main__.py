import sys
import getopt

"""Parquet Generator entrypoint"""
from .core import ParquetGenerator

if __name__ == "__main__":
    tsv_file = ''

    opts, args = getopt.getopt(sys.argv[1:], '', ['tsv-file='])
    for opt, arg in opts:
        if opt in ('--tsv-file'):
            tsv_file = arg

    gen = ParquetGenerator(tsv_file)
    dataframe = gen.dataframe()
    gen.partition(dataframe)
    new_blocks = gen.get_new_block_dataset()
    print(new_blocks.read().to_pandas())

    new_burn_blocks = gen.get_new_burn_block_dataset()
    print(new_burn_blocks.read().to_pandas())
