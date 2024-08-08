"""
database_uploader.py

"""
import csv
import config
import logging
import oracledb
from pathlib import Path

logger = logging.getLogger(__name__)


def update_table_with_csv(config: dict, csv_file: Path):
    """
    Update or insert records from the CSV file into the database table.
    """
    # adjust the number of rows to be inserted in each iteration
    # to meet your memory and performance requirements
    batch_size = 10000

    sql = config.canvas_tables.get(csv_file.stem).get("db_query")
    num_columns = len(config.canvas_tables.get(csv_file.stem).get("fields"))

    with oracledb.connect(
        user=config.oracle_username,
        password=config.oracle_password,
        host=config.db_host,
        port=config.db_port,
        service_name=config.db_service
    ) as connection:

        with connection.cursor() as cursor:

            # Predefine the memory areas to match the table definition.
            # This can improve performance by avoiding memory reallocations.
            # Here, one parameter is passed for each of the columns.
            # "None" is used for the ID column, since the size of NUMBER isn't
            # variable.  The "25" matches the maximum expected data size for the
            # NAME column

            # cursor.setinputsizes(None, 25)

            with open(csv_file, 'r', encoding="utf-8") as csv_stream:
                csv_reader = csv.reader(csv_stream, delimiter=',')

                # skip the header row
                next(csv_reader)
                
                data = []
                parameters = []
                records_affected = 0
                for line in csv_reader:
                    i = 0
                    while i < num_columns:
                        parameters.append(line[i])
                        i += 1

                    data_tuple = tuple(parameters)
                    data.append(data_tuple)
                    parameters = []
                    if len(data) % batch_size == 0:
                        cursor.executemany(sql, data, batcherrors=True, arraydmlrowcounts=True)
                        data = []
                        records_affected += sum(cursor.getarraydmlrowcounts())
                if data:
                    cursor.executemany(sql, data, batcherrors=True, arraydmlrowcounts=True)
                    records_affected += sum(cursor.getarraydmlrowcounts())

            connection.commit()
            logger.info(f"Table [canvas_{csv_file.stem}] had [{records_affected}] rows updated or inserted.")

            for error in cursor.getbatcherrors():
                logger.error("Error", error.message, "at row offset", error.offset)


def main(config: dict) -> None:
    """
    Main function to process CSV files and update the database.
    """
    if not config.final_path.is_dir():
        logger.error(f"The path {config.final_path} is not a valid directory.")
        raise ValueError(f"The path {config.final_path} is not a valid directory.")
    
    csv_files = [file for file in config.final_path.glob("*.csv")]

    for csv_file in csv_files:
        try:
            update_table_with_csv(config, csv_file)
        except Exception as e:
            logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    user_config = config.get_config()
    main(user_config)



















# def insert_dataframe_to_oracle(df, table_name, connection_string):
#     """
#     Insert records from a Pandas DataFrame into an Oracle table.

#     :param df: Pandas DataFrame containing the data to be inserted.
#     :param table_name: Name of the Oracle table where data will be inserted.
#     :param connection_string: Oracle connection string in the form of 'user/password@host:port/service_name'.
#     """
#     # Establish a connection to the Oracle database
#     connection = oracledb.connect(connection_string)
#     cursor = connection.cursor()

#     # Get column names from DataFrame
#     columns = df.columns
#     column_names = ', '.join(columns)
    
#     # Prepare SQL query for inserting data
#     placeholders = ', '.join([':' + str(i + 1) for i in range(len(columns))])
#     sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

#     # Insert each row from the DataFrame into the table
#     for index, row in df.iterrows():
#         cursor.execute(sql, tuple(row))
    
#     # Commit the transaction
#     connection.commit()

#     # Close the cursor and connection
#     cursor.close()
#     connection.close()

# # Example usage
# if __name__ == "__main__":
#     # Example DataFrame
#     data = {'column1': [1, 2], 'column2': ['value1', 'value2']}
#     df = pd.DataFrame(data)

#     # Connection string and table name
#     # connection_string = 'user/password@host:port/service_name'
#     # table_name = 'your_table_name'

#     # Insert data into Oracle table
#     insert_dataframe_to_oracle(df, table_name, connection_string)








# # with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
# with oracledb.connect(user=un, password=pw, host="admin-bannerdb", port=1521, service_name="TEST") as connection:
#     with connection.cursor() as cursor:
#         sql = """select sysdate from dual"""
#         for r in cursor.execute(sql):
#             print(r)

# import getpass
# import oracledb

# pw = getpass.getpass("Enter password: ")

# connection = oracledb.connect(
#     user="stanisim",
#     password=pw,
#     dsn="localhost/xepdb1")

# print("Successfully connected to Oracle Database")

# cursor = connection.cursor()

# # Create a table

# cursor.execute("""
#     begin
#         execute immediate 'drop table todoitem';
#         exception when others then if sqlcode <> -942 then raise; end if;
#     end;""")

# cursor.execute("""
#     create table todoitem (
#         id number generated always as identity,
#         description varchar2(4000),
#         creation_ts timestamp with time zone default current_timestamp,
#         done number(1,0),
#         primary key (id))""")

# # Insert some data

# rows = [ ("Task 1", 0 ),
#          ("Task 2", 0 ),
#          ("Task 3", 1 ),
#          ("Task 4", 0 ),
#          ("Task 5", 1 ) ]

# cursor.executemany("insert into todoitem (description, done) values(:1, :2)", rows)
# print(cursor.rowcount, "Rows Inserted")

# connection.commit()

# # Now query the rows back
# for row in cursor.execute('select description, done from todoitem'):
#     if (row[1]):
#         print(row[0], "is done")
#     else:
#         print(row[0], "is NOT done")