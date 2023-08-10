import sys
import getopt

from .core import CoreEventsProcessor
from parquet_generator.processors.new_block_processor import NewBlockProcessor
from parquet_generator.processors.new_burn_block_processor import NewBurnBlockProcessor
from parquet_generator.processors.attachments_processor import AttachmentsProcessor

if __name__ == "__main__":
    tsv_file = ''

    opts, args = getopt.getopt(sys.argv[1:], '', ['tsv-file='])
    for opt, arg in opts:
        if opt in ('--tsv-file'):
            tsv_file = arg

    processor = CoreEventsProcessor(tsv_file)
    processor.tsv_entity()

    df = processor.prepare_dataframe()
    df_to_reorg, df_remainder = processor.split(df)

    # partition events to be re-orged
    processor.partition(df_to_reorg)

    # create a parquet file of remainder events
    processor.to_parquet(df_remainder)

    # process new_block events to canonical data
    new_block_data = processor.get_new_block_dataset()
    new_block_processor = NewBlockProcessor(new_block_data)
    canonical_indexes = new_block_processor.to_canonical()
    new_block_processor.save_dataset()

    # process attachments_new events to canonical data
    attachments_data = processor.get_attachments_dataset()
    AttachmentsProcessor(attachments_data).to_canonical(canonical_indexes).save_dataset()

    # process new_burn_block events to canonical data
    new_burn_blocks_data = processor.get_new_burn_block_dataset()
    NewBurnBlockProcessor(new_burn_blocks_data).to_canonical().save_dataset()