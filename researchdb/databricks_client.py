import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

load_dotenv()

def get_databricks_client():

    return WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )

def fetch_research_data(query):
    client = get_databricks_client()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

    try:
        response = client.statement_execution.execute_statement(
          warehouse_id=warehouse_id,
          statement=query
        )

        if response.status.state == StatementState.SUCCEEDED:
            return response.result.data_array
        else:
            print(f"Query Failed: {response.status.error.message}")
            return None

    except Exception as e:
        print(f"Error connecting to Databricks: {e}")
        return None
