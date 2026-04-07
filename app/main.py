from fastapi import FastAPI
from app.database import Base, engine
from app.models import video, result
from app.routes import video as video_router  # fix this line

# Create tables on startup
Base.metadata.create_all(bind=engine)

app = FastAPI(title="VIRA Backend", version="1.0.0")

app.include_router(video_router.router, prefix="/api/v1")  # and this line

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "VIRA API"}