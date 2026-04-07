from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.sql import func
from app.database import Base


class InferenceResult(Base):
    __tablename__ = "inference_results"

    id               = Column(Integer, primary_key=True, index=True)
    video_id         = Column(Integer, ForeignKey("videos.id"), nullable=False)
    mrf_id           = Column(String(100), nullable=False)

    # Per-class IN counts (all 19 classes, default 0)
    pet_bottle_clear  = Column(Integer, default=0)
    pet_bottle_green  = Column(Integer, default=0)
    ldpe_clear        = Column(Integer, default=0)
    ldpe_hm           = Column(Integer, default=0)
    ldpe_black        = Column(Integer, default=0)
    hdpe_bottle       = Column(Integer, default=0)
    metal_can         = Column(Integer, default=0)
    milk_packet       = Column(Integer, default=0)
    pp_bag            = Column(Integer, default=0)
    mlp_packet        = Column(Integer, default=0)
    sachet            = Column(Integer, default=0)
    tetrapack         = Column(Integer, default=0)
    cardboard_brown   = Column(Integer, default=0)
    paper_box         = Column(Integer, default=0)
    coconut_shell     = Column(Integer, default=0)
    footwear          = Column(Integer, default=0)
    idpe_colored      = Column(Integer, default=0)
    hard_plastic      = Column(Integer, default=0)
    other             = Column(Integer, default=0)

    # Summary
    total_count      = Column(Integer, default=0)
    processed_frames = Column(Integer, default=0)
    total_frames     = Column(Integer, default=0)
    output_video_url = Column(String(500), nullable=True)
    created_at       = Column(DateTime, default=func.now())