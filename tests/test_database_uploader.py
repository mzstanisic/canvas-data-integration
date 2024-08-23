import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
import csv
import io
from canvas_data_integration.database_uploader import update_table_with_csv


class TestUpdateTableWithCsv(unittest.TestCase):

    @patch("oracledb.connect")
    @patch(
        "builtins.open",
        new_callable=unittest.mock.mock_open,
        read_data="col1,col2\nval1,val2\n",
    )
    def test_update_table_with_csv(self, mock_open, mock_oracledb_connect):
        # Setup mocks
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_oracledb_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        user_config = {
            "oracle_username": "user",
            "oracle_password": "password",
            "db_host": "localhost",
            "db_port": "1521",
            "db_service": "service",
            "canvas_tables": {
                "test_csv": {
                    "db_query": "INSERT INTO test_table (col1, col2) VALUES (:1, :2)",
                    "fields": ["col1", "col2"],
                }
            },
            "batch_size": 1,
            "final_path": Path("."),
        }

        csv_file = Path("test_csv.csv")

        # Call the function
        update_table_with_csv(user_config, csv_file)

        # Assert cursor.executemany was called correctly
        mock_cursor.executemany.assert_called_once_with(
            "INSERT INTO test_table (col1, col2) VALUES (:1, :2)",
            [("val1", "val2")],
            batcherrors=True,
            arraydmlrowcounts=True,
        )

        # Assert commit was called on the connection
        mock_connection.commit.assert_called_once()

        # Assert logger.info was called
        with self.assertLogs("your_module", level="INFO") as cm:
            update_table_with_csv(user_config, csv_file)
            self.assertIn(
                "INFO:your_module:Table [canvas_test_csv] had [1] rows updated or inserted.",
                cm.output,
            )


if __name__ == "__main__":
    unittest.main()
