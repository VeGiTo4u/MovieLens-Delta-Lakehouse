"""README link checks for local project documentation paths."""

import re
from pathlib import Path


def test_readme_local_markdown_links_exist():
    root = Path(__file__).resolve().parents[2]
    readme = root / "README.md"
    text = readme.read_text()

    missing = []
    for raw_target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
        if raw_target.startswith(("http://", "https://", "mailto:")):
            continue

        target = raw_target.split("#", 1)[0]
        if not target:
            continue

        if not (root / target).exists():
            missing.append(raw_target)

    assert missing == []
