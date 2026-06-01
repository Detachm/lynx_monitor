from __future__ import annotations

import gzip
import tempfile
import unittest
from pathlib import Path

from beast_market.runtime_state import prune_runtime_state


class RuntimeStatePruneTest(unittest.TestCase):
    def test_prune_runtime_state_dry_run_reports_archives_and_deletes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_jsonl = root / "20260530" / "00700.HK" / "processed-events.jsonl"
            expired_jsonl = root / "20260520" / "00700.HK" / "processed-events.jsonl"
            current_jsonl = root / "20260601" / "00700.HK" / "processed-events.jsonl"
            for path in (old_jsonl, expired_jsonl, current_jsonl):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"ok":true}\n', encoding="utf-8")

            result = prune_runtime_state(root, reference_date="20260601")

            self.assertTrue(result.dry_run)
            self.assertIn(str(old_jsonl.with_suffix(".jsonl.gz")), result.archived_paths)
            self.assertIn(str(root / "20260520"), result.deleted_paths)
            self.assertTrue(old_jsonl.exists())
            self.assertTrue(expired_jsonl.exists())

    def test_prune_runtime_state_compresses_jsonl_and_removes_expired_days_when_confirmed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_jsonl = root / "20260530" / "00700.HK" / "processed-events.jsonl"
            expired_dir = root / "20260520"
            expired_jsonl = expired_dir / "00700.HK" / "processed-events.jsonl"
            for path in (old_jsonl, expired_jsonl):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"ok":true}\n', encoding="utf-8")

            result = prune_runtime_state(root, reference_date="20260601", dry_run=False, confirm=True)

            gz_path = old_jsonl.with_suffix(".jsonl.gz")
            self.assertFalse(old_jsonl.exists())
            self.assertTrue(gz_path.exists())
            with gzip.open(gz_path, "rt", encoding="utf-8") as handle:
                self.assertEqual(handle.read(), '{"ok":true}\n')
            self.assertFalse(expired_dir.exists())
            self.assertIn(str(gz_path), result.archived_paths)
            self.assertIn(str(expired_dir), result.deleted_paths)


if __name__ == "__main__":
    unittest.main()
