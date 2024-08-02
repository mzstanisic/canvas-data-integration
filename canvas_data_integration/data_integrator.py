"""
data_integrator.py


"""

import pandas as pd

async def main(dataframes: dict) -> pd.DataFrame:
    # first, rename columns for each dataframe
    df_enrollment_terms = dataframes["enrollment_terms"].rename(columns={
        'value.sis_source_id': 'TermCode'
    })
    df_courses = dataframes["courses"].rename(columns={
        'value.sis_source_id': 'CourseTermCRNID',
        'value.name': 'CourseName'
    })

    # then, filter each dataframe with the specific where clauses
    df_enrollment_terms = df_enrollment_terms.query("`TermCode` == '202430_B7'")
    # df_courses = df_courses.query("`value.workflow_state` == 'available' and `value.is_public` == True")
    df1_filtered = df1[df1['name'] == 'Term A']
    df2_filtered = df2[df2['status'] == 'available']

    # then, join the dataframes
    result = pd.merge(
        left=df_enrollment_terms,
        right=df_courses,
        left_on='key.id',
        right_on='value.enrollment_term_id'
    )

    # Perform the join on multiple columns
    result = pd.merge(
        left=df1_filtered,
        right=df2_filtered,
        left_on=['key_id', 'term_code'],
        right_on=['key_id', 'term_code']
    )


    # and output the final result
    # one full file with all columns
    # one final file with only the selected columns of the final query
    result.to_csv(Path(merged), index=False)