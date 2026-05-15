from app.database import SessionLocal
from app.models.video import Video
from app.workers.tasks import process_video
from datetime import datetime, timedelta

def recover_stuck_jobs():
    db = SessionLocal()
    try:
        # Find videos stuck in queued for more than 10 minutes
        cutoff = datetime.utcnow() - timedelta(minutes=30)
        stuck = db.query(Video).filter(
            Video.status == "queued",
            Video.created_at < cutoff
        ).all()

        for video in stuck:
            blob_name = f"{video.mrf_id}/{video.filename}"
            process_video.delay(video.id, blob_name)
            print(f"Re-queued: {video.id} - {video.filename}")

        return len(stuck)
    finally:
        db.close()