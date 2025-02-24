# main.py
from fastapi import FastAPI
from view.profiling_view import router as profiling_router

app = FastAPI()

# Include the router with a prefix (optional)
app.include_router(profiling_router, prefix="/api")