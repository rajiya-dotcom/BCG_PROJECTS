import argparse
import time

import queries_components
from queries_components.utils import file_utils, log_utils

logger = log_utils.get_logger()

if __name__ == '__main__':
    start = time.time()
    arg_parser = argparse.ArgumentParser(description='QUERY_DATA_STEPS')
    arg_parser.add_argument('config_filename', help='name of config file for QUERY_DATA_PIPELINE operation')
    args = arg_parser.parse_args()

    config = file_utils.load_yaml_config(f'{args.config_filename}')
    queries_components.run_data_pipeline(
                config=config
            )
    end = time.time()
    logger.info(f"Run time:{end - start}")
