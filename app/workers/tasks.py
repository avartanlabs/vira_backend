from app.workers.celery_app import celery
from app.database import SessionLocal
from app.models.video import Video
from app.models.result import InferenceResult
from app.storage import download_from_blob, delete_from_blob, upload_to_blob
from app.config import settings
from ultralytics import RTDETR
from ultralytics.solutions import object_counter
from ultralytics.utils import LOGGER
from collections import defaultdict
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
def filter_by_class_conf(results, thresholds):
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


# ── Patch counter to use per-class thresholds ─────────────
def patch_counter_model(counter, model, thresholds, device):
    original_track = model.track

    def filtered_track(source, *args, **kwargs):
        kwargs['conf']    = 0.01   # low conf — we filter manually per class
        kwargs['device']  = device
        kwargs['verbose'] = False
        results = original_track(source, *args, **kwargs)
        results = filter_by_class_conf(results, thresholds)
        return results

    counter.model.track = filtered_track


# ── Load config once at module level ──────────────────────
DATA_YAML_PATH = os.path.join(os.path.dirname(settings.MODEL_PATH), "data.yaml")
thresholds, class_names = load_class_config(DATA_YAML_PATH)


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
            5,
            (width, height)
        )

        # 5. Setup counter + patch with per-class thresholds
        line_y       = height // 2
        sample_every = max(1, round(fps / 5))

        print(f"[{video_id}] Initializing counter (line_y={line_y}, sample_every={sample_every})...")

        counter = object_counter.ObjectCounter(
            model=settings.MODEL_PATH,
            region=[(0, line_y), (width, line_y)],
            conf=0.01,       # will be overridden by patch
            device='0',
            show=False,
            verbose=False
        )

        model = RTDETR(settings.MODEL_PATH)
        patch_counter_model(counter, model, thresholds, '0')

        # 6. Track per-class IN counts
        prev_class_in  = defaultdict(int)
        class_in_total = defaultdict(int)

        # 7. Process frames
        frame_idx = 0
        processed = 0

        print(f"[{video_id}] Processing at 5fps (every {sample_every} frames)...")
        while True:
            ret, frame = cap.read()
            if not ret:
                break

            if frame_idx % sample_every == 0:
                solution = counter(frame)
                out.write(solution.plot_im)
                processed += 1

                # Extract per-class IN counts from classwise_count
                classwise = getattr(solution, 'classwise_count', {}) or {}
                for class_name, counts in classwise.items():
                    current_in = counts.get("OUT", 0)
                    prev_in    = prev_class_in[class_name]
                    new_in     = current_in - prev_in
                    if new_in > 0:
                        class_in_total[class_name] += new_in
                    prev_class_in[class_name] = current_in

            frame_idx += 1

        cap.release()
        out.release()

        # 8. Total IN count across all classes
        total_in = sum(class_in_total.values())
        print(f"[{video_id}] Done. Total IN={total_in}")
        for cls, cnt in sorted(class_in_total.items()):
            print(f"  {cls}: {cnt}")

        # 9. Upload output video to Blob
        output_blob_name = f"outputs/{video.mrf_id}/{filename}.mp4"
        output_url       = upload_to_blob(local_output, output_blob_name)
        delete_from_blob(blob_name)

        # Map class names from yaml to DB column names
        # e.g. "pet-bottle-clear" → "pet_bottle_clear"
        def to_col(name):
            return name.replace("-", "_")

        # 10. Save inference result with all class counts
        inference_result = InferenceResult(
            video_id=video_id,
            mrf_id=video.mrf_id,
            total_count=total_in,
            processed_frames=processed,
            total_frames=total_frames,
            output_video_url=output_url,
            # Set each class column — default 0 if not detected
            pet_bottle_clear = class_in_total.get("pet-bottle-clear", 0),
            pet_bottle_green = class_in_total.get("pet-bottle-green", 0),
            ldpe_clear       = class_in_total.get("ldpe-clear", 0),
            ldpe_hm          = class_in_total.get("ldpe-hm", 0),
            ldpe_black       = class_in_total.get("ldpe-black", 0),
            hdpe_bottle      = class_in_total.get("hdpe-bottle", 0),
            metal_can        = class_in_total.get("metal-can", 0),
            milk_packet      = class_in_total.get("milk-packet", 0),
            pp_bag           = class_in_total.get("pp-bag", 0),
            mlp_packet       = class_in_total.get("mlp-packet", 0),
            sachet           = class_in_total.get("sachet", 0),
            tetrapack        = class_in_total.get("tetrapack", 0),
            cardboard_brown  = class_in_total.get("cardboard-brown", 0),
            paper_box        = class_in_total.get("paper-box", 0),
            coconut_shell    = class_in_total.get("coconut-shell", 0),
            footwear         = class_in_total.get("footwear", 0),
            idpe_colored     = class_in_total.get("idpe-colored", 0),
            hard_plastic     = class_in_total.get("hard-plastic", 0),
            other            = class_in_total.get("other", 0),
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
            "total_in": total_in,
            "class_counts": dict(class_in_total)
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