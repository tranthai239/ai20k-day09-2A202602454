"""Fast unit tests, no Docker needed:  python -m unittest discover -s tests   (needs numpy, pillow, pyarrow, deltalake, boto3)."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from PIL import Image, ImageFilter  # noqa: E402

from pipeline import bronze, frames, verify  # noqa: E402


def scene(seed=0, size=(640, 360)) -> np.ndarray:
    """A synthetic road-like image with real edges, deterministic."""
    rng = np.random.default_rng(seed)
    img = np.zeros(size[::-1], dtype=np.uint8)
    img[:] = np.linspace(60, 200, size[0], dtype=np.uint8)
    for _ in range(25):
        x, y, w, h = rng.integers(0, size[0] - 80), rng.integers(0, size[1] - 60), rng.integers(20, 80), rng.integers(10, 60)
        img[y:y + h, x:x + w] = rng.integers(0, 255)
    return img


class FrameSignals(unittest.TestCase):
    def test_dhash_stable_for_near_identical_frames(self):
        a = scene()
        b = np.clip(a.astype(np.int16) + np.random.default_rng(1).integers(-2, 3, a.shape), 0, 255).astype(np.uint8)   # sensor noise
        diff = bin((frames.dhash(a) ^ frames.dhash(b)) & (2**64 - 1)).count("1")
        self.assertLessEqual(diff, 2)

    def test_dhash_differs_for_different_scenes(self):
        diff = bin((frames.dhash(scene(0)) ^ frames.dhash(scene(7))) & (2**64 - 1)).count("1")
        self.assertGreater(diff, 8)

    def test_dhash_fits_signed_bigint(self):
        for s in range(20):
            self.assertTrue(-(2**63) <= frames.dhash(scene(s)) < 2**63)

    def test_blur_score_drops_when_blurred(self):
        sharp = scene()
        blurred = np.asarray(Image.fromarray(sharp).filter(ImageFilter.GaussianBlur(8)))
        self.assertLess(frames.blur_score(blurred), 0.2 * frames.blur_score(sharp))

    def test_black_frame_has_no_edges(self):
        self.assertEqual(frames.blur_score(np.zeros((360, 640), np.uint8)), 0.0)


class Bronze(unittest.TestCase):
    def test_sha256_changes_with_content(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "x.bin"; p.write_bytes(b"a"); h1 = bronze.sha256_file(p)
            p.write_bytes(b"b"); self.assertNotEqual(h1, bronze.sha256_file(p))

    def test_csv_to_table_keeps_everything_as_text_and_adds_lineage(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "t.csv"; p.write_text("a,b\n1,\n2,x\n")
            t = bronze._csv_to_table(p, "drive_z", "abc")
            self.assertEqual(t.column("a").to_pylist(), ["1", "2"])
            self.assertEqual(t.column("b").to_pylist(), ["", "x"])           # empty stays empty text, no NaN, no cast
            self.assertEqual(t.column("_batch_id").to_pylist(), ["abc", "abc"])
            self.assertEqual(t.column("_row_no").to_pylist(), [0, 1])


class Expectations(unittest.TestCase):
    """The verify module's independent counts must equal what the data generator planted."""
    def test_expected_quarantine_counts(self):
        for drive in ("drive_a", "drive_b"):
            e = verify._expected_telemetry(drive)
            self.assertEqual((e["duplicate"], e["speed_missing"], e["speed_out_of_range"]), (20, 30, 3), drive)

    def test_fail_once_switch(self):
        from pipeline import config as C
        with tempfile.TemporaryDirectory() as d:
            C.WORK_DIR = Path(d); os.environ["LAB_FAIL_ONCE"] = "trip-x"
            try:
                with self.assertRaises(RuntimeError):
                    frames._fail_once("trip-x")
                frames._fail_once("trip-x")                                   # second attempt passes
                frames._fail_once("trip-other")                               # other trips are never affected
            finally:
                os.environ.pop("LAB_FAIL_ONCE")


if __name__ == "__main__":
    unittest.main()
