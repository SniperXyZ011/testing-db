import os
import json
import logging
import google.generativeai as genai
import chromadb
from fastapi import HTTPException
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- AI AND CHROMADB INITIALIZATION ---
logging.basicConfig(level=logging.INFO)

try:
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
except Exception as e:
    logging.error(f"Error configuring Gemini API: {e}")

try:
    chroma_client = chromadb.PersistentClient(path="chroma_db")
    collection = chroma_client.get_collection(name="postgres_schema_info")
    logging.info("Successfully connected to ChromaDB and retrieved collection.")
except Exception as e:
    logging.error(f"Error connecting to ChromaDB: {e}")
    collection = None

embedding_model = "models/text-embedding-004"
generation_model = genai.GenerativeModel('gemini-2.5-flash')

# --- AI FUNCTIONS ---
def triage_query(user_query: str) -> dict:
    """
    First AI step: Determines if a query can be answered directly or needs database access.
    """
    prompt = f"""
    You are an AI assistant for an oceanographic database. Analyze the user's query and decide if it requires database access.
    The database contains specific data about ARGO floats: platform numbers, locations, dates, and sensor readings (temperature, salinity, pressure).
    The database only contains data for indian ocean right now so if user asks for data outside indian ocean please respond with direct_answer and provide the answer in response key.
    If the query is a vauge one like give me some data about indian ocean or show me some data then ask the user to be more specific. 
    For a database query, the user should mention about date and the ocean region.

    Respond in JSON format with two keys: "decision" and "response".
    - If the query is a general question, a greeting, or asks for an explanation (e.g., "Hello", "What is an ARGO float?"), set "decision" to "direct_answer" and provide the answer in the "response" key.
    - If the query asks for specific data, plots, or calculations (e.g., "Show me temperatures for float X", "Plot salinity vs temperature"), set "decision" to "database_query" and set "response" to an empty string.

    User Query: "{user_query}"
    """
    try:
        response = generation_model.generate_content(prompt)
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
        return json.loads(cleaned_response)
    except Exception as e:
        logging.error(f"Error in triage_query: {e}")
        return {"decision": "database_query", "response": ""}

def generate_sql_from_query(user_query: str) -> str:
    """
    Generates a SQL query for PostgreSQL/PostGIS using schema context from ChromaDB.
    """
    if not collection:
        raise HTTPException(status_code=500, detail="ChromaDB collection not available.")

    # Step 1: Get schema context from ChromaDB
    try:
        query_embedding = genai.embed_content(model=embedding_model, content=[user_query])['embedding']
        results = collection.query(query_embeddings=query_embedding, n_results=10)
        schema_context = "\n".join(doc for doc in results['documents'][0])
        logging.info(f"Retrieved schema context for query: {user_query}")
    except Exception as e:
        logging.error(f"Error getting schema context: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve schema context.")

    # Step 2: Generate SQL using the context
    prompt = f"""
    You are an expert PostgreSQL/PostGIS programmer. Given the schema context and a user question, generate a single, executable SQL SELECT query.

    **Schema Context:**
    {schema_context}

    **User Question:**
    "{user_query}"

    **Instructions:**
    - Only generate the SQL query. No explanations or markdown.
    - Use PostgreSQL JSON operators (->, ->>, #>) for JSON columns.
    - For spatial data, use PostGIS functions (e.g., ST_AsGeoJSON, ST_Distance).
    - Always return a valid SQL SELECT statement.

    **PostgreSQL Query:**
    """
    try:
        response = generation_model.generate_content(prompt)
        sql_query = response.text.strip().replace("```sql", "").replace("```", "")
        if not sql_query.upper().startswith("SELECT"):
            raise ValueError("Generated query is not a SELECT statement.")
        logging.info(f"Generated SQL: {sql_query}")
        return sql_query
    except Exception as e:
        logging.error(f"Error generating SQL: {e}")
        raise HTTPException(status_code=500, detail="AI query generation failed.")

def interpret_results_for_frontend(user_query: str, db_results: list) -> dict:
    """
    Converts database results into natural language, Plotly data, or tabular data.
    Adds safeguard for large datasets.
    """

    # --- Step 0: Check for too many rows ---
    MAX_ROWS = 1000
    if len(db_results) > MAX_ROWS:
        return {
            "natural_language_response": (
                f"Your query returned {len(db_results)} rows. "
                "Please narrow down your request by specifying float numbers, parameters, or date ranges "
                "so that fewer rows are returned."
            ),
            "plot_data": None,
            "table_data": None
        }
    
    prompt = f"""
    You are a data analysis assistant. You have a user's question and the corresponding data from a PostgreSQL/PostGIS database.
    Your task is to create a final JSON response. The JSON must contain:
    1. "natural_language_response": A friendly, natural language answer summarizing the key findings.
    2. "plot_data": A Plotly-compatible JSON object if a "plot", "graph", or "chart" is requested. Otherwise, it MUST be null.
    3. "table_data": The raw data formatted for a frontend table. Populate this if the user asks to "show", "list", "get", or "find" data and does not ask for a plot.

    **User's Question:**
    "{user_query}"

    **Data from Database:**
    {json.dumps(db_results, indent=2)}

    **Instructions for your response:**
    - For plotting requests, create a plot_data object. If only one series, use index for x-axis.
    - For tabular requests, populate table_data with raw rows.
    - Always provide a natural_language_response.

    **Final JSON:**
    """
    try:
        response = generation_model.generate_content(prompt)
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
        return json.loads(cleaned_response)
    except Exception as e:
        logging.error(f"Error in interpret_results_for_frontend: {e}")
        return {
            "natural_language_response": "I was able to retrieve the data, but had trouble interpreting it for a final answer.",
            "plot_data": None,
            "table_data": db_results
        }
