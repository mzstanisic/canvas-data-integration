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
from canvas_data_integration import canvas, processing, database

if __name__ == "__main__":

    # TODO: get config figured out

    asyncio.run(canvas.main())      # no arguments
    asyncio.run(processing.main())  # accepts the resulting files, or temp file path? and maybe a format
    asyncio.run(database.main())    # accepts the final dataframe
