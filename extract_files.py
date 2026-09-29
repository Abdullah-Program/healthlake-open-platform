import re
import stat
import sys
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: python extract_files.py <markdown_file> [target_dir]")
    sys.exit(1)

md_file = Path(sys.argv[1])
if not md_file.exists():
    print(f"Error: {md_file} not found")
    sys.exit(1)

md = md_file.read_text(encoding="utf-8")
target = Path(sys.argv[2] if len(sys.argv) > 2 else ".")
pattern = re.compile(r"^#### FILE: (\S+)[ \t]*\n(`{4,})[^\n]*\n(.*?)\n\2[ \t]*$", re.M | re.S)

count = 0
for m in pattern.finditer(md):
    rel, _fence, body = m.groups()
    dest = target / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w", encoding="utf-8", newline="\n") as f:
        f.write(body + "\n")
    if rel.endswith(".sh"):
        try:
            dest.chmod(dest.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        except Exception:
            pass
    print("wrote", dest)
    count += 1

print(f"\nSuccessfully extracted {count} files into {target.resolve()}")
