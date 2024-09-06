"""
The running script.
    * First, retrieves the data from Canvas
    * Second, imports the data from the generated data files into dataframes, flattens,
      renames, and drops columns, finally outputting final data files
    * Third, merges data from final data files into database tables
"""

import asyncio
import config
import canvas_extractor
import data_transformer
import database_uploader


async def run_pipeline():
    """
    Runs the main project pipeline.
    """
    # get the processed user config
    user_config = config.get_config()

    # extracts data files from Canvas
    await canvas_extractor.main(user_config)

    # gets data into dataframes from the Canvas data files,
    # then flattens, drops extraneous columns, and exports them to data/final
    data_transformer.main(user_config)

    # merges final data files to database
    database_uploader.main(user_config)


if __name__ == "__main__":
    asyncio.run(run_pipeline())
