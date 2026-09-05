#!/usr/bin/env python3
"""Compare qualify_browse JSONL with a read-only source Git repository."""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def verify(source: Path, evidence: Path, revision: str) -> dict:
    def git(*args: str) -> bytes:
        return subprocess.check_output(["git", "-C", str(source), *args])

    records = [json.loads(line) for line in evidence.read_text().splitlines()]
    if not records or records[-1]["kind"] != "complete":
        raise ValueError("reader did not complete")
    head = git("rev-parse", revision).decode().strip()
    snapshot, = [r for r in records if r["kind"] == "snapshot"]
    assert snapshot["commit"] == head, "snapshot revision differs"
    assert snapshot["tree"] == git("rev-parse", f"{head}^{{tree}}").decode().strip()
    refs = [r for r in records if r["kind"] == "ref"]
    assert refs == [{"kind": "ref", "name": "refs/heads/main", "oid": head}]

    expected = {}
    for line in git("ls-tree", "-r", "-t", "-z", head).split(b"\0"):
        if not line:
            continue
        metadata, path = line.split(b"\t", 1)
        mode, kind, oid = metadata.split()
        expected[path] = (int(mode, 8), kind.decode(), oid.decode())
    entries = [r for r in records if r["kind"] == "entry"]
    actual = {bytes(r["path"]): (r["mode"], r["oid"]) for r in entries}
    assert len(actual) == len(entries), "duplicate directory entry"
    assert actual == {p: (v[0], v[2]) for p, v in expected.items()}, "tree differs"

    commits = [r for r in records if r["kind"] == "commit"]
    history = git("rev-list", "--first-parent", "--max-count=1000", head).decode().splitlines()
    assert [r["oid"] for r in commits] == history, "paginated history differs"
    for commit in commits:
        raw = git("cat-file", "commit", commit["oid"])
        header, message = raw.split(b"\n\n", 1)
        fields = [line.split(b" ", 1) for line in header.splitlines() if not line.startswith(b" ")]
        parents = [v.decode() for k, v in fields if k == b"parent"]
        assert parents == commit["parents"], "commit parents differ"
        values = dict(fields)
        assert values[b"tree"].decode() == commit["tree"], "commit tree differs"
        assert list(message) == commit["message"], "commit message differs"
        identity, timestamp, _ = values[b"author"].rsplit(b" ", 2)
        name, email = identity.rsplit(b" <", 1)
        assert list(name) == commit["author_name"], "author differs"
        assert list(email[:-1]) == commit["author_email"], "author email differs"
        assert int(timestamp) == commit["author_seconds"], "author time differs"
        assert int(values[b"committer"].rsplit(b" ", 2)[1]) == commit["committer_seconds"]

    samples = [r for r in records if r["kind"] == "blob"]
    assert len(samples) == min(128, sum(v[1] == "blob" for v in expected.values()))
    for blob in samples:
        oid = expected[bytes(blob["path"])][2]
        assert blob["oid"] == blob["computed_oid"] == oid, "sample content differs"
        assert len(git("cat-file", "blob", oid)) == blob["bytes"], "sample size differs"

    errors, = [r for r in records if r["kind"] == "errors"]
    assert errors == {"kind": "errors", "directory_as_blob": "EntryNotBlob", "cancelled": "Cancelled"}

    archive = [r for r in records if r["kind"] == "archive"]
    assert len(archive) == len(expected), "archive count differs"
    assert {bytes(r["path"]) for r in archive} == expected.keys(), "archive paths differ"
    blob_count = total_bytes = 0
    for entry in archive:
        mode, kind, oid = expected[bytes(entry["path"])]
        assert entry["mode"] == mode, "archive mode differs"
        if kind == "blob":
            assert entry["computed_oid"] == oid, "archive content differs"
            blob_count += 1
            total_bytes += entry["bytes"]
        else:
            assert entry["computed_oid"] is None and entry["bytes"] is None
    return {"status": "passed", "head": head, "commits_checked": len(commits),
            "tree_entries_checked": len(entries), "blobs_checked": blob_count,
            "blob_bytes_checked": total_bytes, "direct_blob_samples": len(samples),
            "reader": records[-1], "snapshot": snapshot, "error_checks": errors,
            "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--revision", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = verify(args.source, args.evidence, args.revision)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
