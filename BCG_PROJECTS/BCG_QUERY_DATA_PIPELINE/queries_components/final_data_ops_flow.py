# Python import
import os

# Local import
from .utils import log_utils
from .base_function import DataPipeline, BCGQueryAnalysis
from .analysis import CaseStudyDataQueryPipeline

logger = log_utils.get_logger(app_name=__name__)

def run_data_pipeline(**kwargs):
    """
    Driver function : To run a config driven operation
    """
    config = kwargs['config'].CASESTUDYQUERIES
    input_file_path = os.path.join(config.base_data_path, config.raw_data_path)
    output_path = os.path.join(config.base_data_path, config.final_dump_path_for_validation_report)

    serial_ops = kwargs['config'].CASESTUDYQUERIES.serial_ops

    for serial_op in serial_ops:
        components_list = serial_op.components
        logger.info(f"You are planning to execute the modules: {components_list}")
        
        for compo in components_list:
            logger.info(f"Serial operations on dataset: {compo.input_file_name}.")
            logger.info(f"Working on :{compo.component}")
            component = compo.component
            kwargs['component'] = component
            QueriesComponent = BCGQueryAnalysis.create(component, kwargs)

            file_names_info = compo.input_file_name
            relative_path_for_input_file = file_names_info.relative_path
            files_dict = file_names_info.file_names
            full_path_added_files_dict = {}
            for file_info in files_dict:
                input_file = os.path.join(input_file_path, relative_path_for_input_file, files_dict[file_info])
                full_path_added_files_dict[file_info] = input_file
            file_read_kwargs = file_names_info.file_read_kwargs or {}

            logger.info(f"Files with updated file path: {full_path_added_files_dict}.")

            QueriesComponent.run(compo.operations, full_path_added_files_dict, output_path, file_read_kwargs)
        logger.info("Queries are Done!!!")

