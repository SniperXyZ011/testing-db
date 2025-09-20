import os
import psycopg2
import psycopg2.extras
import logging
from fastapi import HTTPException

# --- DB FUNCTION ---
def execute_sql_query(sql: str) -> list:
    """
    Executes the generated SQL query on PostgreSQL/PostGIS and returns the results.
    """
    try:
        with psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            dbname=os.getenv("POSTGRES_DB"),
            port=os.getenv("POSTGRES_PORT")
        ) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                logging.info(f"SQL query returned {len(results)} results.")
                return results
    except psycopg2.Error as err:
        logging.error(f"Postgres Execution Error: {err}")
        raise HTTPException(status_code=500, detail=f"Database query execution failed: {err}")
