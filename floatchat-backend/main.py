

# --- FASTAPI APP SETUP ---
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import router

app = FastAPI(
    title="FloatChat Backend API",
    description="An advanced API that uses a multi-step AI agent to answer questions about ARGO data (Postgres/PostGIS).",
    version="3.0.0"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
