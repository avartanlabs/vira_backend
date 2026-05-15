from app.workers.celery_app import celery
from app.database import SessionLocal
from app.models.video import Video
from app.models.result import InferenceResult
from app.storage import download_from_blob, delete_from_blob, upload_to_blob
from app.config import settings
from ultralytics import RTDETR
from ultralytics.utils import LOGGER
from collections import defaultdict, Counter
import numpy as np
import cv2
import os
import yaml
import logging

logging.getLogger("azure").setLevel(logging.WARNING)
LOGGER.setLevel(logging.WARNING)


# ── Load per-class config from data.yaml ──────────────────
def load_class_config(yaml_path, num_classes=19, default_conf=0.55):
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    class_conf = data.get("class_conf", {})
    thresholds = [float(class_conf.get(i, default_conf)) for i in range(num_classes)]
    names = data.get("names", {})
    return thresholds, names


# ── Per-class confidence filter ───────────────────────────
def filter_detections(results, thresholds):
    boxes = results[0].boxes
    if boxes is None or len(boxes) == 0:
        return results
    cls_ids = boxes.cls.cpu().numpy().astype(int)
    confs   = boxes.conf.cpu().numpy()
    keep = np.array([
        confs[i] >= thresholds[cls_ids[i]]
        for i in range(len(cls_ids))
    ])
    results[0] = results[0][keep]
    return results


# ── Load config once at module level ──────────────────────
DATA_YAML_PATH = settings.DATA_YAML_PATH
thresholds, class_names = load_class_config(DATA_YAML_PATH)

# Disappear buffer — number of frames a track must be absent before counting
DISAPPEAR_BUFFER = 50


@celery.task(bind=True)
def process_video(self, video_id: int, blob_name: str):
    db = SessionLocal()
    filename     = os.path.basename(blob_name)
    local_input  = f"/tmp/input_{filename}"
    local_output = f"/tmp/output_{filename}.mp4"

    try:
        # 1. Update status
        video = db.query(Video).filter(Video.id == video_id).first()
        video.status = "processing"
        db.commit()

        # 2. Download from Blob
        print(f"[{video_id}] Downloading {blob_name}...")
        download_from_blob(blob_name, local_input)

        # 3. Open video
        cap          = cv2.VideoCapture(local_input)
        width        = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height       = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps          = cap.get(cv2.CAP_PROP_FPS) or 25
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        total_frames = max(0, total_frames) if total_frames > 0 else 0

        if width == 0 or height == 0:
            raise ValueError(f"Could not read video dimensions for {local_input}")

        # 4. Output writer
        out = cv2.VideoWriter(
            local_output,
            cv2.VideoWriter_fourcc(*"mp4v"),
            5,  # output at 5fps
            (width, height)
        )

        # 5. Load model
        print(f"[{video_id}] Loading model...")
        model = RTDETR(settings.MODEL_PATH)

        # 6. ByteTrack majority voting state
        track_history      = defaultdict(list)   # track_id → [class_id, ...]
        prev_track_ids     = set()
        disappeared_counter = defaultdict(int)   # track_id → frames absent
        final_counts       = Counter()           # class_name → count

        # 7. Process frames at 5fps
        sample_every = max(1, round(fps / 5))
        frame_idx    = 0
        processed    = 0

        print(f"[{video_id}] Processing at 5fps (every {sample_every} frames)...")

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            if frame_idx % sample_every == 0:
                # Run tracking with low conf — filter per class manually
                results = model.track(
                    frame,
                    conf=0.01,
                    device='0',
                    tracker="bytetrack.yaml",
                    persist=True,
                    verbose=False
                )

                # Apply per-class confidence filter
                results = filter_detections(results, thresholds)

                current_track_ids = set()

                if results[0].boxes is not None and results[0].boxes.id is not None:
                    boxes     = results[0].boxes
                    track_ids = boxes.id.int().cpu().tolist()
                    class_ids = boxes.cls.int().cpu().tolist()

                    for track_id, class_id in zip(track_ids, class_ids):
                        track_history[track_id].append(class_id)
                        current_track_ids.add(track_id)

                # Track disappeared objects
                disappeared_ids = prev_track_ids - current_track_ids
                for tid in disappeared_ids:
                    disappeared_counter[tid] += 1

                # Count tracks that have been gone long enough
                for tid in list(disappeared_counter.keys()):
                    if tid in current_track_ids:
                        # Reappeared — reset counter
                        del disappeared_counter[tid]
                    elif disappeared_counter[tid] >= DISAPPEAR_BUFFER:
                        # Gone long enough — majority vote and count
                        if track_history[tid]:
                            majority_cls = Counter(track_history[tid]).most_common(1)[0][0]
                            class_name   = model.names[majority_cls]
                            final_counts[class_name] += 1
                        del track_history[tid]
                        del disappeared_counter[tid]

                prev_track_ids = current_track_ids

                # Draw and write annotated frame
                annotated = results[0].plot()
                out.write(annotated)
                processed += 1

            frame_idx += 1

        # 8. Count any tracks still active at end of video
        for tid, predictions in track_history.items():
            if predictions:
                majority_cls = Counter(predictions).most_common(1)[0][0]
                class_name   = model.names[majority_cls]
                final_counts[class_name] += 1

        cap.release()
        out.release()

        # 9. Summary
        total_count = sum(final_counts.values())
        print(f"[{video_id}] Done. Total={total_count}")
        for cls, cnt in sorted(final_counts.items()):
            if cnt > 0:
                print(f"  {cls}: {cnt}")

        # 10. Upload output video to Blob
        output_blob_name = f"outputs/{video.mrf_id}/{filename}.mp4"
        output_url       = upload_to_blob(local_output, output_blob_name)
        delete_from_blob(blob_name)

        # 11. Save inference result
        inference_result = InferenceResult(
            video_id=video_id,
            mrf_id=video.mrf_id,
            total_count=total_count,
            processed_frames=processed,
            total_frames=total_frames,
            output_video_url=output_url,
            pet_bottle_clear = final_counts.get("pet-bottle-clear", 0),
            pet_bottle_green = final_counts.get("pet-bottle-green", 0),
            ldpe_clear       = final_counts.get("ldpe-clear", 0),
            ldpe_hm          = final_counts.get("ldpe-hm", 0),
            ldpe_black       = final_counts.get("ldpe-black", 0),
            hdpe_bottle      = final_counts.get("hdpe-bottle", 0),
            metal_can        = final_counts.get("metal-can", 0),
            milk_packet      = final_counts.get("milk-packet", 0),
            pp_bag           = final_counts.get("pp-bag", 0),
            mlp_packet       = final_counts.get("mlp-packet", 0),
            sachet           = final_counts.get("sachet", 0),
            tetrapack        = final_counts.get("tetrapack", 0),
            cardboard_brown  = final_counts.get("cardboard-brown", 0),
            paper_box        = final_counts.get("paper-box", 0),
            coconut_shell    = final_counts.get("coconut-shell", 0),
            footwear         = final_counts.get("footwear", 0),
            idpe_colored     = final_counts.get("idpe-colored", 0),
            hard_plastic     = final_counts.get("hard-plastic", 0),
            other            = final_counts.get("other", 0),
        )
        db.add(inference_result)

        # 12. Update video record
        video.status           = "done"
        video.total_frames     = total_frames
        video.processed_frames = processed
        db.commit()

        return {
            "status": "done",
            "video_id": video_id,
            "total_count": total_count,
            "class_counts": dict(final_counts)
        }

    except Exception as e:
        db.rollback()
        try:
            video = db.query(Video).filter(Video.id == video_id).first()
            if video:
                video.status = "failed"
                db.commit()
        except Exception:
            pass
        raise self.retry(exc=e, countdown=5, max_retries=3)

    finally:
        for f in [local_input, local_output]:
            if os.path.exists(f):
                os.remove(f)
        db.close()


@celery.task
def recover_stuck_jobs():
    db = SessionLocal()
    try:
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(minutes=10)
        stuck = db.query(Video).filter(
            Video.status == "queued",
            Video.created_at < cutoff
        ).all()
        for video in stuck:
            blob_name = f"{video.mrf_id}/{video.filename}"
            process_video.delay(video.id, blob_name)
            print(f"[RECOVER] Re-queued video {video.id} - {video.filename}")
        return len(stuck)
    finally:
        db.close()