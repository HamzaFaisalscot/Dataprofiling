from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from view.profiling_view import router as profiling_router

app = FastAPI()

# 𝗠𝗨𝗦𝗧 𝗯𝗲 𝗯𝗲𝗳𝗼𝗿𝗲 𝗿𝗼𝘂𝘁𝗲𝘀
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://0.0.0.0:3004"],  # Exact frontend origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Include routers AFTER middleware
from view.profiling_view import router as profiling_router
app.include_router(profiling_router, prefix="/api")
