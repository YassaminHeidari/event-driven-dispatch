#!/usr/bin/env python3
"""Prints the important lines of a failed build log as one GitHub Actions error annotation,
so the cause shows on the run page without opening the full log."""
import re
import sys

PATTERNS = re.compile(
    r"(ERROR|FAIL|Exception|Caused by|expected|but was|Tests run:.*Fail|BUILD FAILURE|cannot find symbol|"
    r"symbol:|location:|incompatible types|error:|APPLICATION FAILED|Description:|Action:)")


def main(path):
    try:
        with open(path, errors="replace") as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        print(f"::error::{path} not found")
        return
    hits = [l.strip()[:300] for l in lines if PATTERNS.search(l) and " INFO " not in l]
    seen, picked = set(), []
    for l in hits:
        if l not in seen:
            seen.add(l)
            picked.append(l)
    text = "\n".join(picked[:60]) or "\n".join(lines[-40:])
    text = text.replace("%", "%25").replace("\r", "").replace("\n", "%0A")
    print(f"::error title=Failure summary::{text}")


if __name__ == "__main__":
    main(sys.argv[1])
