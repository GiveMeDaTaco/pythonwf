from sqlalchemy import create_engine, MetaData, Table, select, func, and_


def construct_unique_drops_query(table_name):
    # Create an engine that will connect to your database.
    # Replace 'your_connection_string' with your actual connection string.
    engine = create_engine('your_connection_string')
    metadata = MetaData(bind=engine)

    # Reflect table structure from the database.
    table = Table(table_name, metadata, autoload_with=engine)

    # Start with an empty list to collect our case statements.
    case_statements = []
    prior_columns = []

    # Dynamically build case statements considering all prior columns.
    for column in table.c:
        if column.name.startswith('column'):
            conditions = [column == 0] + [prev_column != 0 for prev_column in prior_columns]
            case_statement = func.sum(
                func.case([(and_(*conditions), 1)], else_=0)
            ).label(f'unique_drops_{column.name}')
            case_statements.append(case_statement)
            prior_columns.append(column)

    # Construct the select statement.
    query = select(case_statements)

    return query


# Use the function to construct the query.
table_name = 'your_table'  # Replace with your table name.
query = construct_unique_drops_query(table_name)
print(str(query))
