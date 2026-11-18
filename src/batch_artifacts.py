"""Run-scoped DataFrame snapshots passed between Airflow quality gates.

Only locally generated frames are read. SHA-256 verifies that loading uses
the same bytes approved by the prepared gate, rather than re-reading sources.
"""

import hashlib
import io
import os
from pathlib import Path
import uuid

import pandas as pd


def _root():
    return Path(os.getenv("BATCH_ARTIFACT_DIR", Path(__file__).resolve().parents[1] / "data" / "batches")).resolve()


def save_frames(run_key, stage, frames):
    run_folder = hashlib.sha256(str(run_key).encode()).hexdigest()[:24]
    folder = _root() / run_folder / f"{stage}_{uuid.uuid4().hex[:12]}"
    folder.mkdir(parents=True, exist_ok=False)
    manifest = {}
    for name, frame in frames.items():
        path = folder / f"{name}.pkl"
        frame.to_pickle(path)
        manifest[name] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    return manifest


def load_frames(manifest):
    frames = {}
    root = _root()
    for name, item in manifest.items():
        path = Path(item["path"]).resolve()
        if not path.is_relative_to(root):
            raise ValueError("Batch artifact must remain inside BATCH_ARTIFACT_DIR")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != item["sha256"]:
            raise ValueError(f"Batch artifact changed after validation: {name}")
        frames[name] = pd.read_pickle(io.BytesIO(content))
    return frames


def prepared_frames(dimensions, facts):
    return {**{f"dim_{k}": v for k, v in dimensions.items()},
            **{f"fact_{k}": v for k, v in facts.items()}}


def unpack_prepared(manifest):
    frames = load_frames(manifest)
    return ({k[4:]: v for k, v in frames.items() if k.startswith("dim_")},
            {k[5:]: v for k, v in frames.items() if k.startswith("fact_")})
