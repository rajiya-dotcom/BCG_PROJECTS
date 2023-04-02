# Pyspark import
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Python import
import os
from typing import Dict, List
from dataclasses import dataclass

# Local import 
from .utils import log_utils, log_utils, spark_utils
from .final_data_ops_flow import BCGQueryAnalysis
from .base_function import DataPipeline
 
logger = log_utils.get_logger(app_name=__name__)


@dataclass
@BCGQueryAnalysis.register_component('CaseStudyQuery')

class CaseStudyDataQueryPipeline(DataPipeline):
    def analytics_1(self, df_dict: Dict, output_path: str, file_to_be_used: Dict):
        """
        What to find: 
            Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
        Arguments:
            df_dict: A data dictionary with key as file name and value as DataFrame
            >> sample:{{"Charges_use": Charges_use_df}
            output_path: output location to dump the final result
            file_to_be_used: Files to be used for the current operation by which dataframe can be
                             filtered out from df_dict
        """
        logger.info("Starting the analytics_1 operation")
        primary_person_use_df = df_dict[file_to_be_used["Primary_Person_use_data"]]
        primary_person_use_df = primary_person_use_df.filter((primary_person_use_df["PRSN_INJRY_SEV_ID"]=="KILLED") & (primary_person_use_df["PRSN_GNDR_ID"]=="MALE"))
        grouped_primary_person_use_df = primary_person_use_df.groupby(["PRSN_INJRY_SEV_ID","PRSN_GNDR_ID"]).agg({"DEATH_CNT": "sum"})
        logger.info(f"Sample Analytics_1 output data: {grouped_primary_person_use_df.show(3, truncate=False)}")
        spark_utils.write_data(grouped_primary_person_use_df, output_path)
        logger.info("Analytics_1 operation is done")

    def analytics_2(self, df_dict: Dict, output_path: str, file_to_be_used: Dict):
        """
        What to find: 
            Analysis 2: How many two wheelers are booked for crashes? 
        """
        logger.info("Starting the analytics_2 operation")
        unit_use_df = df_dict[file_to_be_used["Units_use_data"]]
        unit_use_df = unit_use_df.filter(
                                    (unit_use_df["UNIT_DESC_ID"]=="MOTOR VEHICLE") & \
                                    (unit_use_df["VEH_BODY_STYL_ID"].contains("MOTORCYCLE")) & \
                                    (unit_use_df["VEH_PARKED_FL"]=="Y")
            )
        #Lets put all kind of a Motorcycle category as Motorcycle static value
        unit_use_df = unit_use_df.withColumn("VEH_BODY_STYL_ID_motor", F.lit("MOTORCYCLE"))
        grouped_unit_use_df= unit_use_df.groupby(["UNIT_DESC_ID", "VEH_PARKED_FL", "VEH_BODY_STYL_ID_motor"]).agg({"UNIT_NBR": "sum"})
        logger.info(f"Sample Analytics_2 output data: {grouped_unit_use_df.show(3, truncate=False)}")
        logger.info("Analytics_2 operation is done")
        spark_utils.write_data(grouped_unit_use_df, output_path)
    
    def analytics_3(self, df_dict: Dict, output_path: str, file_to_be_used: Dict):
        """
        What to find: 
            Analysis 3: Which state has highest number of accidents in which females are involved? 
        """
        logger.info("Starting the analytics_3 operation")
        primary_person_use_df = df_dict[file_to_be_used["Primary_Person_use_data"]]
        primary_person_use_df = primary_person_use_df.where(primary_person_use_df["PRSN_GNDR_ID"]=="FEMALE")
        primary_person_use_df = primary_person_use_df.groupby(["DRVR_LIC_STATE_ID", "PRSN_GNDR_ID"]) \
                                                    .agg({"TOT_INJRY_CNT": "sum"}) \
                                                    .withColumnRenamed("sum(TOT_INJRY_CNT)", "TOT_INJRY_CNT") \
                                                    .dropna(subset="DRVR_LIC_STATE_ID")
        primary_person_use_df = primary_person_use_df[["DRVR_LIC_STATE_ID", "TOT_INJRY_CNT"]]
        max_value = primary_person_use_df.select(F.max(primary_person_use_df["TOT_INJRY_CNT"])).collect()[0][0]
        primary_person_use_df = primary_person_use_df.filter(primary_person_use_df["TOT_INJRY_CNT"]==max_value)
        primary_person_use_df = primary_person_use_df[["DRVR_LIC_STATE_ID"]]
        logger.info(f"Sample Analytics_3 output data: {primary_person_use_df.show(3, truncate=False)}")
        logger.info("Analytics_3 operation is done")
        spark_utils.write_data(primary_person_use_df, output_path)
    
    def analytics_4(self, df_dict: Dict, output_path: str, file_to_be_used: Dict):
        """
        What to find: 
            Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        logger.info("Starting the analytics_4 operation")
        unit_use_df = df_dict[file_to_be_used["Units_use_data"]]
        unit_use_df = unit_use_df.withColumn("TOT_INJRY_CNT_INCLUDING_DEATH", F.col("TOT_INJRY_CNT")+F.col("DEATH_CNT"))
        unit_use_df = unit_use_df.groupby(["VEH_MAKE_ID"]).agg({"TOT_INJRY_CNT_INCLUDING_DEATH": "sum"}) \
                                .withColumnRenamed("sum(TOT_INJRY_CNT_INCLUDING_DEATH)", "TOT_INJRY_CNT_INCLUDING_DEATH") \
                                .orderBy(F.col("TOT_INJRY_CNT_INCLUDING_DEATH").desc())
        w = Window().partitionBy(F.lit('constant_value_to_get_row_number')).orderBy(F.lit('constant_value_to_get_row_number'))
        unit_use_df = unit_use_df.withColumn("row_num", F.row_number().over(w))
        unit_use_df = unit_use_df.filter(F.col("row_num").between(5,15))
        unit_use_df = unit_use_df[["VEH_MAKE_ID"]]
        logger.info(f"Sample Analytics_4 output data: {unit_use_df.show(3, truncate=False)}")
        logger.info("Analytics_4 operation is done")
        spark_utils.write_data(unit_use_df, output_path)
    
    def analytics_5(self, df_dict: Dict, output_path: str, file_to_be_used: Dict):
        """
        What to find: 
            Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        """
        logger.info("Starting the analytics_5 operation")
        primary_person_use_df = df_dict[file_to_be_used["Primary_Person_use_data"]]
        unit_use_df = df_dict[file_to_be_used["Units_use_data"]]
        primary_person_use_df = primary_person_use_df[["CRASH_ID","UNIT_NBR", "PRSN_ETHNICITY_ID"]].dropna(subset="PRSN_ETHNICITY_ID")
        df = unit_use_df.join(primary_person_use_df, on=["CRASH_ID", "UNIT_NBR"], how="left")
        df = df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count().orderBy(F.col("count").desc()).limit(10)
        df = df[["PRSN_ETHNICITY_ID"]]
        logger.info(f"Sample Analytics_5 output data: {df.show(3, truncate=False)}")
        logger.info("Analytics_5 operation is done")
        spark_utils.write_data(df, output_path)
    
    def analytics_6(self, df_dict: Dict, output_path: str, file_to_be_used: Dict):
        """
        thinking to change : VEH_BODY_STYL_ID as constant car value to make it workable
        What to find: 
            Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number 
                        crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        """
        logger.info("Starting the analytics_6 operation")
        primary_person_use_df = df_dict[file_to_be_used["Primary_Person_use_data"]]
        unit_use_df = df_dict[file_to_be_used["Units_use_data"]]
        primary_person_use_df = primary_person_use_df[["CRASH_ID","UNIT_NBR", "DRVR_ZIP"]].dropna(subset="DRVR_ZIP")
        df = unit_use_df.join(primary_person_use_df, on=["CRASH_ID", "UNIT_NBR"], how="left")
        df = df.filter((F.col("VEH_BODY_STYL_ID").contains("CAR")) | (F.col("VEH_BODY_STYL_ID")=="SPORT UTILITY VEHICLE"))
        df = df.withColumn("contributing_factor", F.concat_ws("#%", F.col("CONTRIB_FACTR_1_ID"), 
                                                                    F.col("CONTRIB_FACTR_2_ID"),
                                                                    F.col("CONTRIB_FACTR_P1_ID")
                                                )
            ).filter(F.col("contributing_factor").contains("ALCOHOL")).withColumn("contributing_factor", F.lit("ALCOHOL")) \
            .dropna(subset="DRVR_ZIP")
        #Making all cars as single unit to get the groupby done on single unit
        df = df.withColumn("VEH_BODY_STYL_ID_cars", F.lit("CAR"))
        df = df.groupby(["contributing_factor", "VEH_BODY_STYL_ID_cars", "DRVR_ZIP"]) \
               .agg({"TOT_INJRY_CNT": "sum"}) \
               .orderBy(F.col("sum(TOT_INJRY_CNT)").desc()) \
               .limit(5)
        df = df[["DRVR_ZIP"]]
        logger.info(f"Sample Analytics_6 output data: {df.show(3, truncate=False)}")
        logger.info("Analytics_6 operation is done")
        spark_utils.write_data(df, output_path)
    
    def analytics_7(self, df_dict: Dict, output_path: str, file_to_be_used: Dict):
        """
        What to find:
            Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is 
                        above 4 and car avails Insurance
        """
        logger.info("Starting the analytics_7 operation")
        unit_use_df = df_dict[file_to_be_used["Units_use_data"]]
        unit_use_df = unit_use_df.withColumn("VEH_DMAG_SCL_1_ID", F.regexp_extract(F.col("VEH_DMAG_SCL_1_ID"), "\\d+", 0))
        unit_use_df = unit_use_df.filter(
                                        (F.col("VEH_DMAG_SCL_1_ID")>4) & \
                                        (F.col("VEH_DMAG_SCL_2_ID")=="NO DAMAGE") & \
                                        (F.col("FIN_RESP_TYPE_ID")=="PROOF OF LIABILITY INSURANCE") & \
                                        ((F.col("VEH_BODY_STYL_ID").contains("CAR")) | (F.col("VEH_BODY_STYL_ID")=="SPORT UTILITY VEHICLE"))
                    )
        unit_use_df = unit_use_df[["CRASH_ID"]]
        distinct_count = unit_use_df.distinct().count()
        distinct_count_df = self.spark.createDataFrame(data=[{"distinct_crash_ids_count": distinct_count}])
        logger.info(f"Sample Analytics_7 output data: {distinct_count_df.show(3, truncate=False)}")
        logger.info("Analytics_7 operation is done")
        spark_utils.write_data(distinct_count_df, output_path)

    def analytics_8(self, df_dict: Dict, output_path: str, file_to_be_used: Dict):
        """
        What to find:
            Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, 
                        has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states 
                        with highest number of offences (to be deduced from the data)
        """
        logger.info("Starting the analytics_8 operation")
        primary_person_use_df = df_dict[file_to_be_used["Primary_Person_use_data"]]
        unit_use_df = df_dict[file_to_be_used["Units_use_data"]]
        charges_use_df = df_dict[file_to_be_used["charges_use_data"]]

        charges_use_df = charges_use_df.filter(F.col("CHARGE").contains("SPEED"))[["CRASH_ID", "UNIT_NBR", "PRSN_NBR", "CHARGE"]]
        primary_person_use_df = primary_person_use_df.filter((F.col("DRVR_LIC_TYPE_ID")=="COMMERCIAL DRIVER LIC.") |
                                                            (F.col("DRVR_LIC_TYPE_ID")=="COMMERCIAL DRIVER LIC.")
        )
        unit_use_df = unit_use_df[["CRASH_ID", "UNIT_NBR", "VEH_COLOR_ID", "VEH_MAKE_ID"]]
        added_veh_info = primary_person_use_df.join(unit_use_df, on=["CRASH_ID", "UNIT_NBR"]) \
                                                    .dropna(subset=("VEH_COLOR_ID", "VEH_MAKE_ID"))
        added_charge_df = added_veh_info.join(charges_use_df, on=["CRASH_ID", "UNIT_NBR", "PRSN_NBR"], how="left") \
                                        .dropna(subset="CHARGE")
        # Make Charge as single constant unit to get the correct sum
        added_charge_df = added_charge_df.withColumn("CHARGE_SPEED", F.lit("SPEED"))
        grouped_added_charge_df = added_charge_df.groupBy("CHARGE_SPEED", "VEH_COLOR_ID").count() \
                                                .orderBy(F.col("count").desc()).limit(10)
        top10_added_veh_colour_df = added_charge_df.join(grouped_added_charge_df, on=["CHARGE_SPEED", "VEH_COLOR_ID"], how="left") \
                                        .dropna(subset="count").drop("count")
        top25_states_df = top10_added_veh_colour_df.groupBy("CHARGE_SPEED", "DRVR_LIC_STATE_ID") \
                                                .count() \
                                                .orderBy(F.col("count").desc()).limit(25)
        top10_added_veh_colour_df = top10_added_veh_colour_df.join(top25_states_df, on=["CHARGE_SPEED", "DRVR_LIC_STATE_ID"], how="left") \
                                        .dropna(subset="count").drop("count")
        top5_vehicle_make_id_charged = top10_added_veh_colour_df.groupBy("CHARGE_SPEED", "VEH_MAKE_ID") \
                                                .count() \
                                                .orderBy(F.col("count").desc()).limit(5)
        top5_vehicle_make_id_charged = top5_vehicle_make_id_charged[["VEH_MAKE_ID"]]
        logger.info(f"Sample Analytics_8 output data: {top5_vehicle_make_id_charged.show(10, truncate=False)}")
        logger.info("Analytics_8 operation is done")
        spark_utils.write_data(top5_vehicle_make_id_charged, output_path)
    
    def run(self, ops: List, files_dict:Dict, output_path:str, file_read_kwargs):
        function_dict = {
        'analytics_1': self.analytics_1,
        'analytics_2': self.analytics_2,
        'analytics_2': self.analytics_2,
        'analytics_3': self.analytics_3,
        'analytics_4': self.analytics_4,
        'analytics_5': self.analytics_5,
        'analytics_6': self.analytics_6,
        'analytics_7': self.analytics_7,
        'analytics_8': self.analytics_8
        }   
        df_dict = {}
        for file in files_dict:
            df_dict[file] = self.read_data(files_dict[file], file_read_kwargs)
        print("files_dict", df_dict)         

        for op in ops:
            logger.info(f"Performing the operation: {op.operation}")
            output_path_for_each_op = os.path.join(output_path, op.output_file_name)
            if 'kwargs' not in op.keys() and 'args' not in op.keys():
                function_dict[op.operation](df_dict, output_path_for_each_op)
            elif 'kwargs' not in op.keys():
                function_dict[op.operation](df_dict, output_path_for_each_op, *op.args)
            elif 'args' not in op.keys():
                function_dict[op.operation](df_dict, output_path_for_each_op, **op.kwargs)
            else:
                function_dict[op.operation](df_dict, output_path_for_each_op, *op.args, **op.kwargs)
        self.spark.sparkContext.stop()
