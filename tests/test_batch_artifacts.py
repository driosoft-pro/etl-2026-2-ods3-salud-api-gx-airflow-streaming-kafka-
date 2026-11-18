import pandas as pd
import pytest

from src.batch_artifacts import load_frames, save_frames


def test_validated_snapshot_keeps_codes_and_detects_tampering(tmp_path, monkeypatch):
    monkeypatch.setenv("BATCH_ARTIFACT_DIR", str(tmp_path))
    original = pd.DataFrame({"code": ["001"], "value": [12]})
    manifest = save_frames("run-a", "raw", {"source": original})
    pd.testing.assert_frame_equal(load_frames(manifest)["source"], original)
    original.loc[0, "value"] = 999
    assert load_frames(manifest)["source"].iloc[0]["value"] == 12
    from pathlib import Path
    with Path(manifest["source"]["path"]).open("ab") as handle:
        handle.write(b"changed")
    with pytest.raises(ValueError, match="changed after validation"):
        load_frames(manifest)


def test_snapshot_path_cannot_escape_artifact_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("BATCH_ARTIFACT_DIR", str(tmp_path / "allowed"))
    with pytest.raises(ValueError, match="inside"):
        load_frames({"source": {"path": str(tmp_path / "outside.pkl"), "sha256": ""}})
