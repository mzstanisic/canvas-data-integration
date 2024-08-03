"""
data_integrator.py


"""

import logging
import pandas as pd
from pathlib import Path
from config import Config

logger = logging.getLogger(__name__)


def rename_dataframe_columns(dataframes: dict) -> dict:
    """
    Renames columns in each DataFrame in the dictionary to include the DataFrame's key as a prefix.
    """
    dataframes_bu = dataframes.copy()  # backup original dataframes in case of failure
    try:
        for key, df in dataframes.items():
            # Create a dictionary to map old column names to new column names
            new_column_names = {}
            
            for column in df.columns:
                # Split column name on the dot and create a new name with the DataFrame key as prefix
                if '.' in column:
                    prefix, name = column.split('.', 1)
                    new_name = f"{key}_{name}"
                    new_column_names[column] = new_name
                else:
                    # Handle columns without a dot
                    new_name = f"{key}_{column}"
                    new_column_names[column] = new_name
            
            # Rename columns
            dataframes[key] = df.rename(columns=new_column_names)

        logger.info("Dataframe columns renamed successfully.")
        return dataframes
    except Exception as e:
        logger.error(f"Failed to rename dataframe columns. Error: {e}")
        return dataframes_bu
    

    # dataframes_bu = dataframes
    # try:
    #     dataframes["course_sections"] = dataframes["course_sections"].rename(columns={
    #         'key.id': 'course_sections_id',
    #         'value.name': 'course_sections_name',
    #         'value.course_id': 'course_sections_course_id',
    #         'value.workflow_state': 'course_sections_workflow_state',
    #         'meta.ts': 'course_sections_timestamp'
    #     })
    #     dataframes["courses"] = dataframes["courses"].rename(columns={
    #         'key.id': 'courses_id',
    #         'value.sis_source_id':'courses_sis_source_id',
    #         'value.name': 'courses_name',
    #         'value.enrollment_term_id': 'courses_enrollment_term_id',
    #         'value.workflow_state': 'courses_workflow_state',
    #         'value.is_public': 'courses_is_public',
    #         'meta.ts': 'course_sections_timestamp'
    #     })
    #     dataframes["enrollment_terms"] = dataframes["enrollment_terms"].rename(columns={
    #         'key.id': 'enrollment_terms_id',
    #         'value.sis_source_id': 'enrollment_terms_sis_source_id',
    #         'value.workflow_state': 'enrollment_terms_workflow_state',
    #         'meta.ts': 'enrollment_terms_timestamp'
    #     })
    #     dataframes["enrollments"] = dataframes["enrollments"].rename(columns={
    #         'key.id': 'enrollments_id',
    #         'value.last_activity_at':'enrollments_last_activity_at',
    #         'value.total_activity_time':'enrollments_total_activity_time',
    #         'value.course_section_id':'enrollments_course_section_id',
    #         'value.course_id':'enrollments_course_id',
    #         'value.role_id':'enrollments_role_id',
    #         'value.user_id':'enrollments_user_id',
    #         'value.sis_pseudonym_id':'enrollments_sis_pseudonym_id',
    #         'value.workflow_state':'enrollments_workflow_state',
    #         'value.type':'enrollments_type',
    #         'meta.ts':'enrollments_timestamp'
    #     })
    #     dataframes["pseudonyms"] = dataframes["pseudonyms"].rename(columns={
    #         'key.id': 'pseudonyms_id',
    #         'value.user_id':'pseudonyms_user_id',
    #         'value.sis_user_id': 'pseudonyms_sis_user_id',
    #         'value.unique_id':'pseudonyms_unique_id',
    #         'value.workflow_state': 'pseudonyms_workflow_state',
    #         'meta.ts': 'pseudonyms_timestamp'
    #     })
    #     dataframes["scores"] = dataframes["scores"].rename(columns={
    #         'key.id': 'scores_id',
    #         'value.current_score': 'scores_current_score',
    #         'value.enrollment_id':'scores_enrollment_id',
    #         'value.workflow_state': 'scores_workflow_state',
    #         'value.course_score':'scores_course_score',
    #         'meta.ts': 'scores_timestamp'
    #     })
    #     dataframes["users"] = dataframes["users"].rename(columns={
    #         'key.id': 'users_id',
    #         'value.workflow_state': 'users_workflow_state',
    #         'value.name': 'users_name',
    #         'meta.ts': 'users_timestamp'
    #     })

    #     logger.info("Dataframe columns renamed successfully.")
    #     return dataframes
    # except Exception as e:
    #     logger.error(f"Failed to rename dataframe columns. Error: {e}")
    #     return dataframes_bu


def clean_bools(dataframes: dict) -> dict:
    # dataframes['scores_course_score'] = dataframe['scores_course_score'].apply(lambda x: True if str(x).lower() == 'true' else False)
    
    def convert_columns_to_boolean(df: pd.DataFrame) -> pd.DataFrame:
        for column in df.columns:
            unique_values = df[column].dropna().unique()
            if set(unique_values).issubset({'True', 'False', True, False, 'true', 'false'}):
                df[column] = df[column].apply(lambda x: True if str(x).lower() == 'true' else False)
        return df

    processed_dataframes = {}
    for key, df in dataframes.items():
        df = convert_columns_to_boolean(df)
        processed_dataframes[key] = df
    return processed_dataframes


# def join_dataframes(dataframes: dict) -> pd.DataFrame:
#     """
#     """
#     try:
#         # student cohort merges
#         merged = pd.merge(
#             left = dataframes["enrollment_terms"],
#             right = dataframes["courses"],
#             left_on = 'enrollment_terms_id',
#             right_on = 'courses_enrollment_term_id'
#         )
#         merged = pd.merge(
#             left = merged,
#             right = dataframes["course_sections"],
#             left_on = 'courses_id',
#             right_on = 'course_sections_course_id'
#         )
#         merged = pd.merge(
#             left = merged,
#             right = dataframes["enrollments"],
#             left_on = ['course_sections_id', 'courses_id'],
#             right_on = ['enrollments_course_section_id', 'enrollments_course_id']
#         )
#         merged = pd.merge(
#             left = merged,
#             right = dataframes["users"],
#             left_on = 'enrollments_user_id',
#             right_on = 'users_id'
#         )
#         merged = pd.merge(
#             left = merged,
#             right = dataframes["pseudonyms"],
#             left_on = ['users_id', 'enrollments_sis_pseudonym_id'],
#             right_on = ['pseudonyms_user_id', 'pseudonyms_id']
#         )
#         merged = pd.merge(
#             left = merged,
#             right = dataframes["scores"],
#             left_on = 'enrollments_id',
#             right_on = "scores_enrollment_id"
#         )
#         logger.info("Merged dataframes successfully.")
#         return merged
#     except Exception as e:
#         logger.error(f"Failed to merge dataframes. Error: {e}")
def join_dataframes(dataframes: dict) -> pd.DataFrame:
    """
    Joins multiple DataFrames into a single DataFrame based on specified keys.
    """
    try:
        # Verify columns exist
        for key, df in dataframes.items():
            print(f"Columns in {key}: {df.columns.tolist()}")

        # Merge DataFrames
        merged = pd.merge(
            left=dataframes["enrollment_terms"],
            right=dataframes["courses"],
            left_on='enrollment_terms_id',
            right_on='courses_enrollment_term_id'
        )
        print("After first merge:", merged.shape) # keys, columns
        print(merged.head())

        merged = pd.merge(
            left=merged,
            right=dataframes["course_sections"],
            left_on='courses_id',
            right_on='course_sections_course_id'
        )
        print("After second merge:", merged.shape)
        print(merged.head())

        merged = pd.merge(
            left=merged,
            right=dataframes["enrollments"],
            left_on=['course_sections_id', 'courses_id'],
            right_on=['enrollments_course_section_id', 'enrollments_course_id']
        )
        print("After third merge:", merged.shape)
        print(merged.head())

        merged = pd.merge(
            left=merged,
            right=dataframes["users"],
            left_on='enrollments_user_id',
            right_on='users_id'
        )
        print("After fourth merge:", merged.shape)
        print(merged.head())

        merged = pd.merge(
            left=merged,
            right=dataframes["pseudonyms"],
            left_on=['users_id', 'enrollments_sis_pseudonym_id'],
            right_on=['pseudonyms_user_id', 'pseudonyms_id']
        )
        print("After fifth merge:", merged.shape)
        print(merged.head())

        merged = pd.merge(
            left=merged,
            right=dataframes["scores"],
            left_on='enrollments_id',
            right_on="scores_enrollment_id"
        )
        print("After sixth merge:", merged.shape)
        print(merged.head())

        logger.info("Merged dataframes successfully.")
        return merged

    except Exception as e:
        logger.error(f"Failed to merge dataframes. Error: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error




def filter_dataframes(dataframe: pd.DataFrame) -> None:
    """
    """
    try:
        # student cohort filters
        dataframe = dataframe[dataframe["courses_workflow_state"] == "available"]
        dataframe = dataframe[dataframe["courses_is_public"] == True]
        dataframe = dataframe[dataframe["course_sections_workflow_state"] == "active"]
        dataframe = dataframe[dataframe["enrollments_role_id"] == 3]
        dataframe = dataframe[dataframe["enrollments_type"] == "StudentEnrollment"]
        dataframe = dataframe[dataframe["enrollments_workflow_state"] == "active"]
        dataframe = dataframe[dataframe["users_workflow_state"] == "registered"]
        dataframe = dataframe[dataframe["pseudonyms_workflow_state"] == "active"]
        dataframe = dataframe[dataframe["scores_workflow_state"] == "active"]
        dataframe = dataframe[dataframe["scores_course_score"] == True]
        dataframe = dataframe[dataframe["enrollment_terms_workflow_state"] == "active"]
        dataframe = dataframe[dataframe["enrollment_terms_sis_source_id"] == "202430_B7"] #TODO: figure out

        logger.info("Filtered main dataframe successfully.")
    except Exception as e:
        logger.error(f"Failed to filter main dataframe. Error: {e}")


def drop_and_rename_columns(dataframe: pd.DataFrame) -> None:
    """
    """
    try:
        # first drop
        columns_to_keep = [
            'enrollment_terms_sis_source_id',
            'courses_sis_source_id',
            'courses_name',
            'course_sections_name',
            'enrollments_last_activity_at',
            'enrollments_total_activity_time',
            'users_name',
            'pseudonyms_sis_user_id',
            'pseudonyms_unique_id',
            'scores_current_score'
        ]

        existing_columns = [col for col in columns_to_keep if col in dataframe.columns]
        dataframe = dataframe[existing_columns]

        # then rename
        dataframe = dataframe.rename(columns={
            'enrollment_terms_sis_source_id':'canvas_term_code',
            'courses_sis_source_id':'canvas_course_term_crn_id',
            'courses_name':'canvas_course_name',
            'course_sections_name':'canvas_course_section_name',
            # TODO: reserved for teacher XID
            # TODO: reserved for teacher name
            'enrollments_last_activity_at': 'canvas_last_activity_date',
            'enrollments_total_activity_time':'canvas_total_activity_time',
            'users_name':'canvas_name',
            'pseudonyms_sis_user_id':'canvas_xid',
            'pseudonyms_unique_id':'canvas_email',
            'scores_current_score':'canvas_score'
        })
        
        logger.info("Dropped unnecessary columns, and renamed columns of main dataframe.")
    except Exception as e:
        logger.error(f"Failed to drop and rename columns of main dataframe. Error: {e}")


def main(dataframes: dict, config: Config) -> pd.DataFrame:
    """
    """
    # rename the selectd dataframe columns for further processing
    dataframes = rename_dataframe_columns(dataframes)

    # clean up bools for all tables
    dataframes = clean_bools(dataframes)

    # save CSV files to data/final
    for key, df in dataframes.items():
        final_dir = config.final_path / f"{key}.csv"
        df.to_csv(final_dir, index=False)

    # join our dataframes per our main query, and drop any unnecessary columns
    final_dataframe = join_dataframes(dataframes)
    final_dir = config.final_path / "canvas_full.csv"
    final_dataframe.to_csv(final_dir, index=False)
    logger.info(f"Output full Canvas CSV to {final_dir}")

    # filter the final dataframe per our main query
    filter_dataframes(final_dataframe)
    final_dir = config.final_path / "canvas_full_filtered.csv"
    final_dataframe.to_csv(final_dir, index=False)
    logger.info(f"Output full-filtered Canvas CSV to {final_dir}")

    # drop unnecessary columns
    drop_and_rename_columns(final_dataframe)
    final_dir = config.final_path / "canvas_final.csv"
    final_dataframe.to_csv(final_dir, index=False)
    logger.info(f"Output final Canvas CSV to {final_dir}")

    return final_dataframe
