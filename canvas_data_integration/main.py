"""
main.py

The running script.
    * First retrieves the data from Canvas in the specified format.
    * Second, imports the data from the generated data files into dataframes and operates on them
    and outputs the final data file.
    * Third, commits the dataframe to a database table for later querying.
      
"""

import asyncio
from canvas_data_integration import config
from canvas_data_integration import canvas_extractor, data_transformer, data_integrator, database_uploader

async def run_pipeline():
    # extracts data files from Canvas in the specified format
    await canvas_extractor.main() #TODO: format, file paths, etc.
    
    # gets data into dataframes from the Canvas data files,
    # flattens and drops extraneous columns, then exports those dataframes as CSV
    # for further operations or review
    dataframes = await data_transformer.main() #TODO: accepts the resulting files, or temp file path? and maybe a format

    # joins and filters dataframes into a main dataframe based on project requirements
    final_dataframe = await data_integrator.main(dataframes)
    
    # final dataframe is uploaded to database
    await database_uploader.main(final_dataframe) # accepts the final dataframe

if __name__ == "__main__":
    asyncio.run(run_pipeline())







    # TODO: get config figured out

    # asyncio.run(canvas.main())      # no arguments
    # asyncio.run(processing.main())  # accepts the resulting files, or temp file path? and maybe a format
    # asyncio.run(database.main())    # accepts the final dataframe
