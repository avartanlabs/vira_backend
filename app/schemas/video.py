from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class VideoUploadResponse(BaseModel):
    job_id: int
    filename: str
    mrf_id: str
    status: str
    message: str

class VideoStatusResponse(BaseModel):
    job_id: int
    filename: str
    mrf_id: str
    status: str
    total_frames: Optional[int]
    processed_frames: Optional[int]
    created_at: datetime

    class Config:
        from_attributes = True