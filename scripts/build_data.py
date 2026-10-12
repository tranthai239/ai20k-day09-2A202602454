#!/usr/bin/env python3
"""Build the Day 9 lab data pack from yaak-ai/L2D-v3 (Apache-2.0). INSTRUCTOR ONLY, needs network.

Students do not run this: the pack it writes to ../data/ is part of the lab.

  python3 scripts/make_dataset.py            # ~250 MB of range reads, ~3-6 min

Output (../data):
  drive_a.mp4, drive_b.mp4      640x360, 10 fps, concatenated trips (one video per "drive")
  telemetry_a.csv               clean-ish schema (speed in m/s, heading in degrees, ISO timestamps)
  telemetry_b.csv               DRIFTED schema (other column names, km/h, epoch ms, +1.5 s clock offset)
  trips.csv                     one row per trip (a.k.a. episode): drive, start/end, lighting, weather
  sources.json                  ingest manifest: what each file is and its known quirks
  _instructor/faults.json       ground truth of every planted fault (hidden from students)

Real parts: video frames, vehicle speed/heading/GPS/accel, pedals/steering, lighting, weather.
Rebuilt: the timeline. L2D episodes are separate drives, so event time is re-based to one
continuous timeline per drive. Planted: video black/freeze/blur/dark, telemetry duplicates,
null burst, spikes, out-of-order rows, and the drifted schema of drive B.
"""
import csv, hashlib, json, subprocess, sys, urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
import numpy as np, pyarrow.compute as pc, pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
DATA, CACHE = ROOT / "data", ROOT / "scripts" / ".cache"
BASE = "https://huggingface.co/datasets/yaak-ai/L2D-v3/resolve/main"
VID = f"{BASE}/videos/observation.images.front_left/chunk-000/file-000.mp4"
CAM = "videos/observation.images.front_left/"
FPS, W, H = 10, 640, 360
TARGET_S = 300           # about 5 minutes per drive
SEED = 9
DRIVES = {
    "drive_a": dict(base=datetime(2025, 10, 7, 8, 0, 0, tzinfo=timezone.utc), vfrom_min=0),
    "drive_b": dict(base=datetime(2025, 10, 8, 14, 30, 0, tzinfo=timezone.utc), vfrom_min=520),
}
# planted video faults, seconds on the drive timeline (inside the drive, chosen to avoid trip edges)
VIDEO_FAULTS = {
    "drive_a": [("black", 100.0, 103.0), ("freeze", 150.0, 156.0), ("blur", 200.0, 204.0)],
    "drive_b": [("black", 60.0, 62.0), ("dark", 140.0, 145.0)],
}
CLOCK_OFFSET_B_MS = 1500


def fetch(url, dest):
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        print("  GET", url.split("/main/")[1], flush=True)
        urllib.request.urlretrieve(url, dest)
    return dest


def run(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode:
        sys.exit(f"ffmpeg failed: {' '.join(cmd)}\n{r.stderr[-1500:]}")


def pick_trips(eps, vfrom_min, used):
    """Greedy, non-overlapping episodes of the first video file, 10-32 s long, until TARGET_S."""
    chosen, end, total = [], -1.0, 0.0
    for r in eps.itertuples():
        if r.vfrom < vfrom_min or r.ep in used or r.vfrom < end or not 100 <= r.len <= 320:
            continue
        chosen.append(r); end = r.vto; total += r.len / FPS
        if total >= TARGET_S:
            break
    return chosen


def main():
    DATA.mkdir(exist_ok=True); (DATA / "_instructor").mkdir(exist_ok=True)
    meta = fetch(f"{BASE}/meta/episodes/chunk-000/file-000.parquet", CACHE / "episodes.parquet")
    e = pq.read_table(meta, columns=["episode_index", "length", "data/file_index", CAM + "file_index",
                                     CAM + "from_timestamp", CAM + "to_timestamp"]).to_pandas()
    e.columns = ["ep", "len", "dfile", "vfile", "vfrom", "vto"]
    eps = e[e.vfile == 0].sort_values("vfrom").reset_index(drop=True)

    used, plan = set(), {}
    for d, cfg in DRIVES.items():
        plan[d] = pick_trips(eps, cfg["vfrom_min"], used); used |= {t.ep for t in plan[d]}
        print(d, len(plan[d]), "trips,", sum(t.len for t in plan[d]) / FPS, "s")

    need_files = sorted({t.dfile for ts in plan.values() for t in ts})
    for f in need_files:
        fetch(f"{BASE}/data/chunk-000/file-{f:03d}.parquet", CACHE / f"data-{f:03d}.parquet")
    want = sorted(used)
    cols = ["episode_index", "frame_index", "observation.state.vehicle", "action.continuous",
            "observation.state.lighting", "observation.state.conditions", "observation.state.precipitation"]
    tabs = [pq.read_table(CACHE / f"data-{f:03d}.parquet", columns=cols,
                          filters=[("episode_index", "in", want)]) for f in need_files]
    import pyarrow as pa
    tab = pa.concat_tables(tabs).to_pandas()
    rng = np.random.default_rng(SEED)
    faults = {"video": {}, "telemetry": {}, "notes": {}}
    trips_out, sources = [], {}

    for d, cfg in DRIVES.items():
        clips, rows, off = [], [], 0.0
        for k, t in enumerate(plan[d]):
            clip = CACHE / f"{d}-clip{k:02d}.mp4"
            if not clip.exists():
                run(["ffmpeg", "-y", "-v", "error", "-ss", f"{t.vfrom:.2f}", "-i", VID, "-t", f"{t.len / FPS:.1f}",
                     "-vf", f"scale={W}:{H},fps={FPS}", "-an", "-c:v", "libx264", "-crf", "23",
                     "-preset", "veryfast", "-pix_fmt", "yuv420p", str(clip)])
            clips.append(clip)
            g = tab[tab.episode_index == t.ep].sort_values("frame_index")
            tt = off + g.frame_index.to_numpy() / FPS
            veh = np.stack(g["observation.state.vehicle"].to_numpy()); act = np.stack(g["action.continuous"].to_numpy())
            for i in range(len(g)):
                rows.append(dict(t=tt[i], speed=veh[i][0], heading=veh[i][1], lat=veh[i][3], lon=veh[i][4], alt=veh[i][5],
                                 acc_x=veh[i][6], acc_y=veh[i][7], gas=act[i][0], brake=act[i][1], steer=act[i][2]))
            mode = lambda c: g[c].mode().iat[0] if len(g) else ""
            b = cfg["base"]
            trips_out.append(dict(trip_id=f"trip-{t.ep:05d}", drive_id=d, t_start_utc=(b + timedelta(seconds=off)).isoformat(),
                                  t_end_utc=(b + timedelta(seconds=off + t.len / FPS)).isoformat(),
                                  lighting=mode("observation.state.lighting"), conditions=mode("observation.state.conditions"),
                                  precipitation=mode("observation.state.precipitation")))
            off += t.len / FPS
        duration = off
        # ---- video: concat + planted faults in one encode pass
        lst = CACHE / f"{d}-concat.txt"; lst.write_text("".join(f"file '{c}'\n" for c in clips))
        vf, fl = [], []
        for kind, a, bnd in VIDEO_FAULTS[d]:
            en = f"between(t,{a},{bnd})"
            if kind == "black":   vf.append(f"drawbox=x=0:y=0:w=iw:h=ih:color=black@1:t=fill:enable='{en}'")
            elif kind == "dark":  vf.append(f"eq=brightness=-0.55:enable='{en}'")
            elif kind == "blur":  vf.append(f"boxblur=luma_radius=18:luma_power=3:enable='{en}'")
            elif kind == "freeze":
                n0, k_ = int(a * FPS), int((bnd - a) * FPS)      # repeat frame n0 k_ times, then drop the k_ frames that follow
                vf.append(f"loop=loop={k_}:size=1:start={n0},select='not(between(n,{n0 + k_ + 1},{n0 + 2 * k_}))',setpts=N/{FPS}/TB")
            fl.append(dict(kind=kind, t_start_s=a, t_end_s=bnd,
                           frames_at_2fps=[i for i in range(int(duration * 2)) if a <= i / 2 < bnd]))
        out = DATA / f"{d}.mp4"
        run(["ffmpeg", "-y", "-v", "error", "-f", "concat", "-safe", "0", "-i", str(lst), "-vf", ",".join(vf) or "null",
             "-r", str(FPS), "-an", "-c:v", "libx264", "-crf", "30", "-preset", "veryfast", "-pix_fmt", "yuv420p",
             "-movflags", "+faststart", str(out)])
        faults["video"][d] = fl
        # ---- telemetry: jittered timestamps, planted faults, drifted schema for drive B
        n = len(rows)
        for r in rows:
            r["t"] = r["t"] + rng.uniform(-0.02, 0.02)
        dup = sorted(rng.choice(np.arange(50, n - 50), 20, replace=False).tolist())
        for i in reversed(dup):
            rows.insert(i, dict(rows[i]))                                   # retry duplicates (exact copy)
        null_start = 1200 if d == "drive_a" else 900
        for i in range(null_start, null_start + 30):
            rows[i]["speed"] = None                                         # sensor dropout
        spikes = [400, 1700, 2500]
        for i in spikes:
            rows[i]["speed"] = 900.0                                        # impossible speed (raw km/h; 250 m/s in drive A)
        ooo = list(range(2000, 2015)) if d == "drive_a" else list(range(1500, 1515))
        seg = rows[ooo[0]:ooo[-1] + 1]; rows[ooo[0]:ooo[-1] + 1] = seg[::-1]  # out-of-order arrival
        # make sure planted duplicates are exact: compute AFTER all mutations, from final rows
        faults["telemetry"][d] = dict(n_rows=len(rows), n_distinct_rows_before_dups=n, duplicate_rows=len(dup),
                                      null_speed_rows=30, speed_spikes=len(spikes), out_of_order_rows=len(ooo),
                                      null_burst_from_row=null_start)
        b = cfg["base"]
        if d == "drive_a":
            with open(DATA / "telemetry_a.csv", "w", newline="") as f:
                w = csv.writer(f); w.writerow(["ts_utc", "speed_mps", "heading_deg", "lat", "lon", "alt_m", "acc_x", "acc_y",
                                                "gas", "brake", "steer"])
                for r in rows:
                    ts = (b + timedelta(seconds=r["t"])).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                    w.writerow([ts, "" if r["speed"] is None else f"{r['speed'] / 3.6:.3f}", f"{r['heading']:.4f}", f"{r['lat']:.6f}",
                                f"{r['lon']:.6f}", f"{r['alt']:.1f}", f"{r['acc_x']:.3f}", f"{r['acc_y']:.3f}",
                                f"{r['gas']:.3f}", f"{r['brake']:.3f}", f"{r['steer']:.3f}"])
        else:
            with open(DATA / "telemetry_b.csv", "w", newline="") as f:
                w = csv.writer(f); w.writerow(["timestamp_ms", "spd_kmh", "hdg", "latitude", "longitude", "altitude", "ax", "ay",
                                                "throttle_pct", "brake_pct", "steering"])
                for r in rows:
                    ms = int((b + timedelta(seconds=r["t"])).timestamp() * 1000) + CLOCK_OFFSET_B_MS
                    w.writerow([ms, "" if r["speed"] is None else f"{r['speed']:.2f}", f"{r['heading']:.4f}", f"{r['lat']:.6f}",
                                f"{r['lon']:.6f}", f"{r['alt']:.1f}", f"{r['acc_x']:.3f}", f"{r['acc_y']:.3f}",
                                f"{r['gas'] * 100:.1f}", f"{r['brake'] * 100:.1f}", f"{r['steer']:.3f}"])
        sources[d] = dict(video=f"{d}.mp4", telemetry="telemetry_a.csv" if d == "drive_a" else "telemetry_b.csv",
                          duration_s=round(duration, 1), video_start_utc=b.isoformat(), fps=FPS,
                          telemetry_clock_offset_ms=0 if d == "drive_a" else CLOCK_OFFSET_B_MS)
    with open(DATA / "trips.csv", "w", newline="") as f:
        w = csv.DictWriter(f, list(trips_out[0])); w.writeheader(); w.writerows(trips_out)
    (DATA / "sources.json").write_text(json.dumps(dict(
        source="yaak-ai/L2D-v3 (Apache-2.0), front_left camera, re-encoded to 640x360 10 fps; timeline re-based per drive",
        drives=sources), indent=2))
    faults["notes"] = dict(drive_b_schema="columns renamed; speed in km/h (drive A is m/s); throttle/brake in percent; timestamp in epoch ms; "
                                          "clock runs 1500 ms ahead of the video clock",
                           video_dark="'dark' is low light (mean luma about 15), NOT black: expected is_dark=true, is_black=false; a pipeline should flag it, not drop it",
                           duplicate_rule="duplicates are exact copies of the previous row after the sort; sorted by ts the copy is adjacent")
    (DATA / "_instructor" / "faults.json").write_text(json.dumps(faults, indent=2))
    for p in sorted(DATA.glob("*")):
        if p.is_file(): print(f"{p.name:20s} {p.stat().st_size / 1e6:7.1f} MB  sha256 {hashlib.sha256(p.read_bytes()).hexdigest()[:12]}")


if __name__ == "__main__":
    main()
