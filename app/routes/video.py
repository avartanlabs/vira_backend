import os
import aiofiles
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.video import Video
from app.schemas.video import VideoUploadResponse, VideoStatusResponse
from app.storage import upload_to_blob
from app.workers.tasks import process_video

router = APIRouter()

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@router.post("/upload", response_model=VideoUploadResponse)
async def upload_video(
    file: UploadFile = File(...),
    mrf_id: str = Form(...),
    db: Session = Depends(get_db)
):
    if not file.filename.endswith((".mp4",".h264", ".avi", ".mkv")):
        raise HTTPException(status_code=400, detail="Invalid file type")

    # Save chunks to local temp file first
    temp_path = os.path.join(UPLOAD_DIR, f"{mrf_id}_{file.filename}")
    async with aiofiles.open(temp_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):  # 1MB chunks
            await f.write(chunk)

    # Upload to Azure Blob
    blob_name = f"{mrf_id}/{file.filename}"
    blob_url = upload_to_blob(temp_path, blob_name)

    # Delete local temp file
    os.remove(temp_path)

    # Create DB record
    video = Video(
        filename=file.filename,
        mrf_id=mrf_id,
        status="queued",
        file_path=blob_url
    )
    db.add(video)
    db.commit()
    db.refresh(video)

    # Queue job with blob name
    process_video.delay(video.id, blob_name)

    return VideoUploadResponse(
        job_id=video.id,
        filename=video.filename,
        mrf_id=video.mrf_id,
        status=video.status,
        message="Video queued for processing"
    )


@router.get("/status/{job_id}", response_model=VideoStatusResponse)
def get_status(job_id: int, db: Session = Depends(get_db)):
    video = db.query(Video).filter(Video.id == job_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Job not found")
    return video


@router.get("/stats/{mrf_id}")
def get_mrf_stats(mrf_id: str, db: Session = Depends(get_db)):
    videos = db.query(Video).filter(Video.mrf_id == mrf_id).all()
    return {
        "mrf_id": mrf_id,
        "total_videos": len(videos),
        "done": sum(1 for v in videos if v.status == "done"),
        "processing": sum(1 for v in videos if v.status == "processing"),
        "failed": sum(1 for v in videos if v.status == "failed"),
        "queued": sum(1 for v in videos if v.status == "queued"),
    }