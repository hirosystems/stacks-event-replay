import sys
import gc
import getopt

from .core import CoreEventsProcessor
from event_replay.processors.new_block_processor import NewBlockProcessor
from event_replay.processors.new_burn_block_processor import NewBurnBlockProcessor
from event_replay.processors.attachments_processor import AttachmentsProcessor

if __name__ == "__main__":
    tsv_file = ''

    opts, args = getopt.getopt(sys.argv[1:], '', ['tsv-file='])
    for opt, arg in opts:
        if opt in ('--tsv-file'):
            tsv_file = arg

    processor = CoreEventsProcessor(tsv_file)
    # create canonical indexes
    canonical_indexes = processor.tsv_entity()
    # augment dataframe
    df = processor.prepare_dataframe()
    # split dataframe
    df_core, df_remainder = processor.split(df)

    del df
    gc.collect()

    # partition prunable events
    processor.partition_prunable(df_core)

    # create remainder parquet file
    processor.remainder_to_parquet(df_remainder)

    # process new_block events to canonical data
    NewBlockProcessor().to_canonical(df_core)

    # process attachments_new events to canonical data
    AttachmentsProcessor().to_canonical(df_core, canonical_indexes)

    # process new_burn_block events to canonical data
    NewBurnBlockProcessor().to_canonical(df_core)
