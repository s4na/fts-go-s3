import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


class CompareBaselineTest(unittest.TestCase):
    def test_differences_and_incompatible_data(self):
        metrics = {"queries": 2, "requests": 5, "received_body_bytes": 100,
                   "false_negatives": 0, "false_positives": 1, "p50_ms": 3, "p95_ms": 8}
        baseline = {"documents": 1002, "shards": 256, "layout": "shards", "bloom": False,
                    "documents_sha256": "docs-hash", "queries_sha256": "queries-hash",
                    "uncached": metrics, "cached_manifest": dict(metrics)}
        current = json.loads(json.dumps(baseline))
        current["uncached"]["requests"] = 7
        with tempfile.TemporaryDirectory() as directory:
            old = pathlib.Path(directory) / "old.json"
            new = pathlib.Path(directory) / "new.json"
            old.write_text(json.dumps(baseline), encoding="utf-8")
            new.write_text(json.dumps(current), encoding="utf-8")
            script = pathlib.Path(__file__).with_name("compare_baseline.py")
            result = subprocess.run([sys.executable, str(script), str(old), str(new)],
                                    capture_output=True, text=True, check=True)
            self.assertIn("| uncached.requests | 5 | 7 | +2 |", result.stdout)
            current["shards"] = 16
            new.write_text(json.dumps(current), encoding="utf-8")
            result = subprocess.run([sys.executable, str(script), str(old), str(new)],
                                    capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("incompatible shards", result.stderr)


if __name__ == "__main__":
    unittest.main()
