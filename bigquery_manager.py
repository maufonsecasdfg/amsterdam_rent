from google.cloud import bigquery

client = bigquery.Client()

def fetch_data(query):
    query_job = client.query(query)
    result = query_job.result()
    return [dict(row) for row in result]

def insert_rows(dataset_id, table_id, rows_to_insert):
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    table = client.get_table(table_ref)  # Fetch table schema

    # Insert rows
    errors = client.insert_rows_json(table, rows_to_insert)
    if errors:
        raise RuntimeError(f"Encountered errors while inserting rows: {errors}")
    print("Rows successfully inserted.")
    
def update_rows(dataset_id, table_id, updates):
    """
    Updates rows in a BigQuery table.

    Args:
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        updates (dict): A dictionary of updates in the format:
            {'column_name_of_key': {'key_value': {'column_to_edit_value': 'new_value'}}}
    
    Example:
        updates = {
            'id': {
                1: {'name': 'Updated Name', 'age': 30},
                2: {'name': 'Another Name', 'age': 40}
            }
        }
    """
    for key_column, key_updates in updates.items():
        for key_value, column_updates in key_updates.items():
            set_clause = ", ".join(
                f"{col} = '{val}'" for col, val in column_updates.items()
            )
            query = f"""
                UPDATE `{client.project}.{dataset_id}.{table_id}`
                SET {set_clause}
                WHERE {key_column} = '{key_value}'
            """
            query_job = client.query(query)
            query_job.result()  # Wait for the query to complete
            print(f"Updated row where {key_column} = {key_value}")


