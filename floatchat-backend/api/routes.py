from fastapi import APIRouter, HTTPException
from core.models import QueryRequest
from core.ai import triage_query, generate_sql_from_query, interpret_results_for_frontend
from core.db import execute_sql_query
import logging

router = APIRouter()

@router.post("/query")
async def handle_query(request: QueryRequest):
    user_query = request.query
    logging.info(f"--- New Query Received: {user_query} ---")

    try:
        triage_result = triage_query(user_query)
        decision = triage_result.get("decision")
        logging.info(f"Triage decision: {decision}")

        if decision == "direct_answer":
            return {
                "natural_language_response": triage_result.get("response"),
                "plot_data": None,
                "table_data": None,
                "generated_sql": None
            }

        elif decision == "database_query":
            generated_sql = generate_sql_from_query(user_query)
            query_results = execute_sql_query(generated_sql)

            if not query_results:
                return {
                    "natural_language_response": "I found no data matching your query. Please try asking in a different way.",
                    "plot_data": None,
                    "table_data": None,
                    "generated_sql": generated_sql
                }

            final_response = interpret_results_for_frontend(user_query, query_results)
            final_response["generated_sql"] = generated_sql

            logging.info("--- Query Processed Successfully ---")
            return final_response

        else:
            raise HTTPException(status_code=500, detail="Invalid decision from triage model.")

    except HTTPException as e:
        logging.error(f"HTTP Exception: {e.detail}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error in handle_query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/")
def read_root():
    return {"message": "FloatChat Agent Backend (Postgres/PostGIS) is running."}
