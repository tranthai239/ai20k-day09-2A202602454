#!/usr/bin/env python3
"""Tải và giải nén gói dữ liệu:  python3 scripts/fetch_data.py [url]

Đường dẫn lấy từ tham số, hoặc từ data/MANIFEST.json (giảng viên điền một lần). Tự kiểm tra sha256 của file zip.
"""
import hashlib
import json
import sys
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
m = json.loads((ROOT / "data" / "MANIFEST.json").read_text())
url = sys.argv[1] if len(sys.argv) > 1 else m.get("url", "")
if not url.startswith("http"):
    sys.exit("Chưa có đường dẫn tải. Hãy dùng: python3 scripts/fetch_data.py <url>  (hoặc tải tay rồi giải nén, xem README.md mục 3)")
dest = ROOT / m.get("zip_name", "day09-data.zip")
print(f"tải {url} ...")
urllib.request.urlretrieve(url, dest)
h = hashlib.sha256(dest.read_bytes()).hexdigest()
if m.get("zip_sha256") and h != m["zip_sha256"]:
    sys.exit(f"sha256 của file tải về ({h[:12]}...) không khớp bản chuẩn ({m['zip_sha256'][:12]}...). Tải lại.")
with zipfile.ZipFile(dest) as z:
    z.extractall(ROOT)
dest.unlink()
print("đã giải nén vào data/. Kiểm tra: python3 scripts/check_data.py")
