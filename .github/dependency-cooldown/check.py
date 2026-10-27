#!/usr/bin/env python3
"""Reject crates.io versions added to Cargo.lock that are younger than the configured cooldown."""

import json
import re
import subprocess
import sys
import tomllib
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"
EXCEPTIONS = Path(__file__).with_name("exceptions.toml")
UNIT_SECONDS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
    "month": 2592000,
}


def min_publish_age() -> tuple[str, timedelta]:
    config = tomllib.loads(Path(".cargo/config.toml").read_text())
    value = config["registry"]["global-min-publish-age"]
    if value == "0":
        return value, timedelta(0)
    match = re.fullmatch(r"(\d+)\s*(second|minute|hour|day|week|month)s?", value)
    if match is None:
        sys.exit(f"cannot parse global-min-publish-age = {value!r}")
    return value, timedelta(seconds=int(match[1]) * UNIT_SECONDS[match[2]])


def crates_io_versions(lockfile: str) -> set[tuple[str, str]]:
    return {
        (package["name"], package["version"])
        for package in tomllib.loads(lockfile)["package"]
        if package.get("source") == CRATES_IO
    }


def publish_time(name: str, version: str) -> datetime | None:
    lower = name.lower()
    match len(lower):
        case 1 | 2:
            path = f"{len(lower)}/{lower}"
        case 3:
            path = f"3/{lower[0]}/{lower}"
        case _:
            path = f"{lower[:2]}/{lower[2:4]}/{lower}"
    request = urllib.request.Request(
        f"https://index.crates.io/{path}",
        headers={"User-Agent": "cachey dependency cooldown check"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        index = response.read().decode()
    for line in index.splitlines():
        entry = json.loads(line)
        if entry["vers"] == version:
            pubtime = entry.get("pubtime")
            return datetime.fromisoformat(pubtime) if pubtime else None
    sys.exit(f"{name} {version} is not in the crates.io index")


def main() -> None:
    base = sys.argv[1] if len(sys.argv) > 1 else "main"
    base_lockfile = subprocess.run(
        ["git", "show", f"{base}:Cargo.lock"], capture_output=True, text=True
    )
    if base_lockfile.returncode:
        sys.exit(base_lockfile.stderr.strip())
    added = crates_io_versions(Path("Cargo.lock").read_text()) - crates_io_versions(
        base_lockfile.stdout
    )
    exceptions = {
        (entry["crate"], entry["version"]): entry["advisory"]
        for entry in tomllib.loads(EXCEPTIONS.read_text()).get("exception", [])
    }
    age_text, age = min_publish_age()
    cutoff = datetime.now(timezone.utc) - age

    too_new = []
    for name, version in sorted(added):
        published = publish_time(name, version)
        if published is None or published <= cutoff:
            continue
        if advisory := exceptions.get((name, version)):
            print(f"{name} {version} is inside the cooldown but allowed: {advisory}")
        else:
            too_new.append(f"{name} {version} (published {published:%Y-%m-%d %H:%M} UTC)")

    if too_new:
        sys.exit(
            f"crates.io versions added since {base} are newer than {age_text}:\n  "
            + "\n  ".join(too_new)
            + f"\nreviewed security releases can be allowed in {EXCEPTIONS}"
        )
    print(f"checked {len(added)} crates.io versions added since {base} against the {age_text} cooldown")


if __name__ == "__main__":
    main()
