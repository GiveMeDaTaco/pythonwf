import sys


class TrackSQL:
    def __init__(self, handler):
        self.handler = handler
        self.tracked_tables = []

    def track_table(self, table_name):
        if table_name not in self.tracked_tables:
            self.tracked_tables.append(table_name)

    def clean_up(self):
        for table in self.tracked_tables:
            try:
                query = f"DROP TABLE {table}"
                self.handler.execute_query(query)
                print(f"Table {table} dropped successfully.")
            except Exception as e:
                print(f"Failed to drop table {table}: {e}", file=sys.stderr)
