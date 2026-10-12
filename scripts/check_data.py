#!/usr/bin/env python3
"""Kiểm tra dữ liệu đã được đặt đúng chỗ chưa:  python3 scripts/check_data.py

Đọc data/MANIFEST.json (danh sách file, kích thước, sha256) và so với thư mục data/.
"""
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


def main() -> int:
    mf = DATA / "MANIFEST.json"
    if not mf.exists():
        print("Thiếu data/MANIFEST.json: bạn đang đứng sai thư mục, hoặc thư mục lab bị thiếu file.")
        return 2
    m = json.loads(mf.read_text())
    bad, ok = [], 0
    for name, info in m["files"].items():
        p = DATA / name
        if not p.exists():
            bad.append(f"  THIẾU   data/{name}")
            continue
        h = hashlib.sha256(p.read_bytes()).hexdigest()
        if h != info["sha256"]:
            bad.append(f"  KHÁC    data/{name}  (không trùng bản chuẩn)")
        else:
            ok += 1
            print(f"  ĐÚNG    data/{name}  ({p.stat().st_size / 1e6:.1f} MB)")
    for line in bad:
        print(line)
    if bad:
        print("\nCách lấy dữ liệu:")
        print("  1. Giải nén gói dữ liệu NGAY TẠI thư mục gốc của lab (zip chứa sẵn thư mục data/):  unzip dataset/day09-data.zip")
        print(f"     (không có file đó? tải từ {m.get('url', 'README.md mục 3')})")
        print("  Hoặc: python3 scripts/fetch_data.py   (tự tải và giải nén nếu giảng viên đã điền đường dẫn)")
        print("  Dự phòng (cần python3, numpy, pyarrow, ffmpeg và mạng, ≈ 5 phút): python3 scripts/build_data.py")
        print("    (dữ liệu tự dựng có thể lệch nhẹ vài khung hình so với bản chuẩn; mọi kiểm tra vẫn dùng được)")
        return 1
    print(f"\nDữ liệu đầy đủ ({ok}/{len(m['files'])} file khớp bản chuẩn). Sẵn sàng: docker compose --profile orchestrate up -d")
    return 0


if __name__ == "__main__":
    sys.exit(main())
