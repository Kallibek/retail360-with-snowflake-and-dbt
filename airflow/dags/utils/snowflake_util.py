from airflow.hooks.base import BaseHook
import snowflake.connector
import logging


def run_snowflake_query(query, **context):
    # Retrieve connection details from the environment variable AIRFLOW_CONN_SNOWFLAKE_DEFAULT
    conn = BaseHook.get_connection("snowflake_default")
    
    # Map connection parameters to the Snowflake connector arguments
    # Note: Adjust the mapping if your connection URI format differs.
    sf_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.host,
        database=conn.schema,  # if set in your connection
        # You can also extract warehouse/role from conn.extra if needed.
    )
    
    try:
        cs = sf_conn.cursor()
        # If query is a list, iterate through each query sequentially
        if isinstance(query, list):
            results = []
            for q in query:
                logging.info(f"Executing query: {q}")
                cs.execute(q)
                result = cs.fetchall()
                results.append(result)
            logging.info(f"All query results:\n{results}")
            return results
        else:
            logging.info(f"Executing query: {query}")
            cs.execute(query)
            result = cs.fetchall()
            logging.info(f"Query result:\n{result}")
            return result
    finally:
        cs.close()
        sf_conn.close()