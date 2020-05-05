# https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/sql.html
# https://github.com/tableau/hyper-api-samples

from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException, TableName

class Extract():
    def __init__(self, path="superstore.hyper"):
        super().__init__()
        self._path = Path(__file__).parent / path
        self._table_name = TableName("Extract", "Extract")
        self._hyper =  HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU)
        self._connection = Connection(  endpoint=self._hyper.endpoint, 
                                        database=self._path)

    def __del__(self):
        self._connection.close()
        self._hyper.close()

    def delete_data(self, date):
        row_count = self._connection.execute_command(
            command= f"DELETE FROM {self._table_name} WHERE order_date>='{date}'"
        )
        print(f"The number of deleted rows in table {self._table_name} "
            f"is {row_count}.\n")

    def read_extract(self):
        print(f"These are all rows in the table {self._table_name}:")
        rows_in_table = self._connection.execute_list_query(query=f"SELECT * FROM {self._table_name}")
        print(rows_in_table)

    def insert_data(self, data):
        with Inserter(self._connection, self._table_name) as inserter:
                inserter.add_rows(rows=data)
                inserter.execute()