import os
import psycopg2
import psycopg2.extras
import chromadb
from chromadb.utils import embedding_functions
from dotenv import load_dotenv
import google.generativeai as genai
import logging

# --- SETUP ---
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()

# Postgres config
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "user": os.getenv("POSTGRES_USER", "argo"),
    "password": os.getenv("POSTGRES_PASSWORD", "mysecretpassword"),
    "dbname": os.getenv("POSTGRES_DB", "argodb"),
}

# Chroma config
CHROMA_PATH = "chroma_db"
COLLECTION_NAME = "postgres_schema_info"

# Google GenAI config
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
embedding_model = "models/text-embedding-004"

# --- CONNECT TO POSTGRES ---
def fetch_schema_info():
    logging.info("Connecting to Postgres...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
            """)
            rows = cur.fetchall()

    logging.info(f"Retrieved {len(rows)} columns from Postgres schema.")
    schema_docs = []
    for row in rows:
        schema_docs.append(f"Table: {row['table_name']}, Column: {row['column_name']}, Type: {row['data_type']}")
    return schema_docs

# --- CONNECT TO CHROMA ---
def update_chroma_collection(docs):
    logging.info("Connecting to ChromaDB...")
    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)

    try:
        collection = chroma_client.get_collection(COLLECTION_NAME)
        logging.info(f"Collection '{COLLECTION_NAME}' found.")
    except Exception:
        logging.info(f"Collection '{COLLECTION_NAME}' not found. Creating new one.")
        collection = chroma_client.create_collection(COLLECTION_NAME)

    # Embed documents with Gemini
    logging.info("Embedding schema docs with Gemini...")
    embeddings = []
    for doc in docs:
        emb = genai.embed_content(model=embedding_model, content=doc)["embedding"]
        embeddings.append(emb)

    # Insert or update docs
    ids = [f"schema_{i}" for i in range(len(docs))]
    collection.upsert(documents=docs, embeddings=embeddings, ids=ids)

    logging.info(f"Upserted {len(docs)} docs into collection '{COLLECTION_NAME}'.")

# --- MAIN ---
if __name__ == "__main__":
    schema_docs = fetch_schema_info()
    update_chroma_collection(schema_docs)
    logging.info("✅ Chroma collection update complete.")
