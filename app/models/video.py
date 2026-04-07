from sqlalchemy import Column, Integer, String, DateTime, Enum, Float
from sqlalchemy.sql import func
from app.database import Base

class Video(Base):
    __tablename__ = "videos"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    mrf_id = Column(String(100), nullable=False)        # which MRF/Pi sent this
    status = Column(Enum("queued", "processing", "done", "failed"), default="queued")
    file_path = Column(String(500), nullable=True)      # where video is saved
    total_frames = Column(Integer, nullable=True)
    processed_frames = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())