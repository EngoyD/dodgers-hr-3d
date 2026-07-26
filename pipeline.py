#!/usr/bin/env python3
"""
Phase 1 data pipeline: pull 2026 Statcast data, filter to LAD home runs,
reconstruct 3D trajectories (drag + Magnus, RK4), emit data/trajectories.json.

Run:  .venv/bin/python pipeline.py
"""

import json
import math
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# CONFIG — physics constants and pipeline knobs, all in one place
# ----------------------------------------------------------------------------
CONFIG = {
    # --- data pull ---
    "season_start": date(2026, 3, 1),      # covers any early international games; game_type filter drops spring training
    "season_end": date.today() - timedelta(days=1),  # through yesterday; nightly runs extend this
    "chunk_days": 7,                       # week-sized requests against the public endpoint
    "max_retries": 4,
    "backoff_base_s": 5.0,                 # exponential: 5, 10, 20, 40
    "raw_dir": Path("data/raw"),
    "out_path": Path("data/trajectories.json"),

    # --- statcast hit-coordinate origin (Savant pixel space) ---
    "hc_x_home": 125.42,
    "hc_y_home": 198.27,

    # --- physics ---
    "gravity_ms2": 9.80665,
    "ball_mass_kg": 0.14529,
    "ball_radius_m": 0.03683,
    "air_density_kgm3": 1.203,             # sea-level 1.225 scaled to ~500 ft elevation (Dodger Stadium)
    "drag_cd_initial": 0.33,               # starting guess; calibrated against median hit_distance_sc
    "drag_cd_bounds": (0.15, 0.60),        # bisection bounds for calibration
    "lift_cl": 0.20,
    "backspin_rpm_min": 2000.0,            # at launch_angle <= 15 deg
    "backspin_rpm_max": 2500.0,            # at launch_angle >= 40 deg
    "backspin_la_lo_deg": 15.0,
    "backspin_la_hi_deg": 40.0,
    "launch_height_ft": 3.0,
    "dt_s": 0.005,                         # RK4 step
    "max_flight_s": 12.0,

    # --- calibration ---
    "calib_tolerance_pct": 5.0,            # required |median residual|
    "calib_bisect_iters": 40,

    # --- output ---
    "downsample_every": 4,                 # keep every 4th RK4 point (~0.02 s spacing)
    "coord_round_ft": 1,                   # decimal places in emitted coords
}

FT_PER_M = 3.280839895
MPH_TO_MS = 0.44704

REQUIRED_FIELDS = [
    "game_date", "player_name", "batter", "pitcher", "pitch_type",
    "release_speed", "launch_speed", "launch_angle", "hc_x", "hc_y",
    "hit_distance_sc", "stand", "des", "home_team", "away_team",
    "game_pk", "at_bat_number",
]


# ----------------------------------------------------------------------------
# Pull + cache
# ----------------------------------------------------------------------------
def week_chunks(start: date, end: date, days: int):
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def pull_season(cfg) -> tuple[pd.DataFrame, list[str]]:
    """Pull all chunks, caching each to parquet. Returns (df, failed_ranges)."""
    from pybaseball import statcast

    cfg["raw_dir"].mkdir(parents=True, exist_ok=True)
    frames, failed = [], []

    for c_start, c_end in week_chunks(cfg["season_start"], cfg["season_end"], cfg["chunk_days"]):
        key = f"statcast_{c_start.isoformat()}_{c_end.isoformat()}.parquet"
        cache_file = cfg["raw_dir"] / key

        if cache_file.exists():
            try:
                df = pd.read_parquet(cache_file)
                print(f"[cache] {key}: {len(df)} rows")
                frames.append(df)
                continue
            except Exception as e:
                print(f"[cache] {key} unreadable ({e}); refetching", file=sys.stderr)

        df = None
        for attempt in range(cfg["max_retries"]):
            try:
                df = statcast(start_dt=c_start.isoformat(), end_dt=c_end.isoformat(), verbose=False)
                break
            except Exception as e:
                wait = cfg["backoff_base_s"] * (2 ** attempt)
                print(f"[pull] {key} attempt {attempt + 1} failed: {e}; retry in {wait:.0f}s",
                      file=sys.stderr)
                time.sleep(wait)

        if df is None:
            failed.append(f"{c_start} .. {c_end}")
            continue

        if len(df) == 0:
            # legitimate empty range (off-days early March); cache empty frame so we never refetch
            df = pd.DataFrame()
        tmp = cache_file.with_suffix(".tmp")
        df.to_parquet(tmp)
        tmp.rename(cache_file)
        print(f"[pull]  {key}: {len(df)} rows")
        frames.append(df)

    non_empty = [f for f in frames if len(f)]
    if not non_empty:
        raise RuntimeError("No Statcast data retrieved for any chunk — aborting (no synthetic fallback).")
    return pd.concat(non_empty, ignore_index=True), failed


# ----------------------------------------------------------------------------
# Filter to LAD home runs
# ----------------------------------------------------------------------------
def lad_home_runs(df: pd.DataFrame) -> pd.DataFrame:
    # regular season + postseason (F=wild card, D=division, L=LCS, W=World Series)
    df = df[df["game_type"].isin(["R", "F", "D", "L", "W"])]
    hr = df[df["events"] == "home_run"].copy()

    # batting team from inning half: Top = away team bats, Bot = home team bats
    batting_team = np.where(hr["inning_topbot"] == "Top", hr["away_team"], hr["home_team"])
    hr["batting_team"] = batting_team
    lad = hr[hr["batting_team"] == "LAD"].copy()

    missing_cols = [c for c in REQUIRED_FIELDS if c not in lad.columns]
    if missing_cols:
        raise RuntimeError(f"Statcast payload missing required columns: {missing_cols}")
    return lad


# ----------------------------------------------------------------------------
# Spray angle
# ----------------------------------------------------------------------------
def add_spray(df: pd.DataFrame, cfg) -> pd.DataFrame:
    dx = df["hc_x"] - cfg["hc_x_home"]
    dy = cfg["hc_y_home"] - df["hc_y"]
    # positive = toward right field (first-base side), in degrees off dead center
    df["spray_deg_field"] = np.degrees(np.arctan2(dx, dy))
    # mirrored so positive = pull side (RHB pull = LF = negative field angle)
    df["spray_deg_pull"] = np.where(df["stand"] == "L", df["spray_deg_field"], -df["spray_deg_field"])
    return df


# ----------------------------------------------------------------------------
# Play video ids: MLB Stats API live feed maps (game_pk, at_bat_number) to the
# play's video GUID; Baseball Savant hosts the clip. Cached per game.
# ----------------------------------------------------------------------------
def fetch_play_ids(df: pd.DataFrame, cfg) -> dict:
    import json as _json
    from urllib.request import urlopen

    feed_dir = cfg["raw_dir"] / "feeds"
    feed_dir.mkdir(parents=True, exist_ok=True)
    out = {}
    for game_pk in sorted(df["game_pk"].astype(int).unique()):
        fp = feed_dir / f"{game_pk}.json"
        if fp.exists():
            game_map = _json.loads(fp.read_text())
        else:
            url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
            feed = None
            for attempt in range(3):
                try:
                    with urlopen(url, timeout=30) as r:
                        feed = _json.load(r)
                    break
                except Exception as e:
                    print(f"[feed] {game_pk} attempt {attempt + 1} failed: {e}", file=sys.stderr)
                    time.sleep(3 * (attempt + 1))
            if feed is None:
                continue
            game_map = {}
            for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
                idx = play.get("about", {}).get("atBatIndex")
                pid = None
                for ev in reversed(play.get("playEvents", [])):
                    if ev.get("playId"):
                        pid = ev["playId"]
                        break
                if idx is not None and pid:
                    game_map[str(idx + 1)] = pid   # statcast at_bat_number is 1-based
            fp.write_text(_json.dumps(game_map))
        for ab, pid in game_map.items():
            out[(game_pk, int(ab))] = pid
    return out


def fetch_video_mp4s(play_ids: dict, cfg, refresh: bool = False) -> dict:
    """Resolve each Savant playId to its raw MP4 URL (for inline playback).
    Cached in one JSON. With refresh=True, HEAD-checks every cached URL and
    re-scrapes the dead ones — MLB rotates clip URLs over time."""
    import json as _json
    import re
    from concurrent.futures import ThreadPoolExecutor
    from urllib.request import Request, urlopen

    cache_fp = cfg["raw_dir"] / "video_mp4s.json"
    cache = _json.loads(cache_fp.read_text()) if cache_fp.exists() else {}
    pat = re.compile(r'https://sporty-clips\.mlb\.com/[^"\']+\.mp4')
    UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"

    if refresh:
        wanted = {pid for pid in set(play_ids.values()) if cache.get(pid)}
        def alive(pid):
            try:
                req = Request(cache[pid], headers={"User-Agent": UA}, method="HEAD")
                with urlopen(req, timeout=15) as r:
                    return pid, r.status == 200
            except Exception:
                return pid, False
        dead = []
        with ThreadPoolExecutor(8) as ex:
            for pid, ok in ex.map(alive, sorted(wanted)):
                if not ok:
                    dead.append(pid)
        if dead:
            print(f"  {len(dead)} cached clip urls rotted — re-resolving")
            for pid in dead:
                del cache[pid]

    def save():
        tmp = cache_fp.with_suffix(".tmp")
        tmp.write_text(_json.dumps(cache))
        tmp.rename(cache_fp)

    fetched = 0
    for pid in sorted(set(play_ids.values())):
        if pid in cache:
            continue
        url = f"https://baseballsavant.mlb.com/sporty-videos?playId={pid}"
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            m = pat.search(html)
            cache[pid] = m.group(0) if m else None
            fetched += 1
            if fetched % 20 == 0:
                save()   # survive interruptions
                print(f"  resolved {fetched} new clips ...")
            time.sleep(0.3)   # gentle on Savant
        except Exception as e:
            print(f"[video] {pid} failed: {e}", file=sys.stderr)
            cache.setdefault(pid, None)
    save()
    return cache


# ----------------------------------------------------------------------------
# Trajectory simulation: quadratic drag + Magnus lift, RK4
# ----------------------------------------------------------------------------
def backspin_rpm(launch_angle_deg: float, cfg) -> float:
    lo, hi = cfg["backspin_la_lo_deg"], cfg["backspin_la_hi_deg"]
    t = (launch_angle_deg - lo) / (hi - lo)
    t = min(max(t, 0.0), 1.0)
    return cfg["backspin_rpm_min"] + t * (cfg["backspin_rpm_max"] - cfg["backspin_rpm_min"])


def simulate(launch_speed_mph, launch_angle_deg, spray_deg_field, cd, cfg):
    """Integrate flight. Returns Nx3 array of [x, y, z] in feet.
    Origin home plate, +x toward CF, +y toward RF line, +z up."""
    v0 = launch_speed_mph * MPH_TO_MS
    la = math.radians(launch_angle_deg)
    phi = math.radians(spray_deg_field)

    vel = np.array([
        v0 * math.cos(la) * math.cos(phi),
        v0 * math.cos(la) * math.sin(phi),
        v0 * math.sin(la),
    ])
    pos = np.array([0.0, 0.0, cfg["launch_height_ft"] / FT_PER_M])

    # pure backspin: horizontal axis perpendicular to initial horizontal velocity
    spin_axis = np.array([math.sin(phi), -math.cos(phi), 0.0])
    cl = cfg["lift_cl"] * (backspin_rpm(launch_angle_deg, cfg) / 2250.0)

    m = cfg["ball_mass_kg"]
    area = math.pi * cfg["ball_radius_m"] ** 2
    rho = cfg["air_density_kgm3"]
    g = np.array([0.0, 0.0, -cfg["gravity_ms2"]])
    k_drag = 0.5 * rho * cd * area / m
    k_lift = 0.5 * rho * cl * area / m

    def accel(v):
        speed = np.linalg.norm(v)
        if speed < 1e-9:
            return g
        drag = -k_drag * speed * v
        lift = k_lift * speed * np.cross(spin_axis, v)
        return g + drag + lift

    dt = cfg["dt_s"]
    n_max = int(cfg["max_flight_s"] / dt)
    pts = [pos.copy()]
    for _ in range(n_max):
        k1v = accel(vel);              k1p = vel
        k2v = accel(vel + 0.5*dt*k1v); k2p = vel + 0.5*dt*k1v
        k3v = accel(vel + 0.5*dt*k2v); k3p = vel + 0.5*dt*k2v
        k4v = accel(vel + dt*k3v);     k4p = vel + dt*k3v
        pos = pos + (dt/6.0)*(k1p + 2*k2p + 2*k3p + k4p)
        vel = vel + (dt/6.0)*(k1v + 2*k2v + 2*k3v + k4v)
        pts.append(pos.copy())
        if pos[2] <= 0.0 and vel[2] < 0.0:
            break
    return np.array(pts) * FT_PER_M


def landing_distance_ft(pts: np.ndarray) -> float:
    return float(math.hypot(pts[-1, 0], pts[-1, 1]))


# ----------------------------------------------------------------------------
# Calibration: bisect Cd so median simulated range matches median hit_distance_sc
# ----------------------------------------------------------------------------
def median_sim_distance(rows, cd, cfg) -> float:
    dists = [
        landing_distance_ft(simulate(r.launch_speed, r.launch_angle, r.spray_deg_field, cd, cfg))
        for r in rows
    ]
    return float(np.median(dists))


def calibrate_cd(df: pd.DataFrame, cfg) -> tuple[float, float, float, float]:
    rows = list(df.itertuples())
    target = float(df["hit_distance_sc"].median())
    lo, hi = cfg["drag_cd_bounds"]

    d_lo = median_sim_distance(rows, lo, cfg)   # low drag -> far
    d_hi = median_sim_distance(rows, hi, cfg)   # high drag -> short
    if not (d_hi <= target <= d_lo):
        raise RuntimeError(
            f"Calibration target {target:.1f} ft outside achievable range "
            f"[{d_hi:.1f}, {d_lo:.1f}] ft for Cd in {cfg['drag_cd_bounds']} — check physics."
        )

    cd = cfg["drag_cd_initial"]
    for _ in range(cfg["calib_bisect_iters"]):
        mid = 0.5 * (lo + hi)
        d_mid = median_sim_distance(rows, mid, cfg)
        if abs(d_mid - target) < 0.05:
            cd = mid
            break
        if d_mid > target:
            lo = mid   # too far -> more drag
        else:
            hi = mid
        cd = mid
    d_final = median_sim_distance(rows, cd, cfg)
    residual_pct = 100.0 * (d_final - target) / target
    return cd, d_final, target, residual_pct


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main(year: int):
    cfg = dict(CONFIG)
    today = date.today()
    cfg["season_start"] = date(year, 3, 1)
    cfg["season_end"] = min(date(year, 11, 15), today - timedelta(days=1))
    cfg["out_path"] = Path(f"data/trajectories_{year}.json")
    print(f"=== Season {year}: {cfg['season_start']} .. {cfg['season_end']} ===")
    df, failed_ranges = pull_season(cfg)
    print(f"\nTotal Statcast rows pulled: {len(df)}")
    if failed_ranges:
        print(f"WARNING — {len(failed_ranges)} date ranges FAILED and are missing:", file=sys.stderr)
        for r in failed_ranges:
            print(f"  {r}", file=sys.stderr)

    lad = lad_home_runs(df)
    reg_total = int((lad["game_type"] == "R").sum())
    ps_total = int((lad["game_type"] != "R").sum())
    season_hr_total = len(lad)
    print(f"LAD home runs found: {reg_total} regular season + {ps_total} postseason")
    if season_hr_total == 0:
        raise RuntimeError("Zero LAD home runs after filtering — aborting.")

    # drop rows unusable for physics; report every drop
    needed = ["launch_speed", "launch_angle", "hc_x", "hc_y", "hit_distance_sc"]
    usable = lad.dropna(subset=needed).copy()
    dropped = season_hr_total - len(usable)
    print(f"Usable for reconstruction: {len(usable)}  (dropped {dropped} lacking {needed})")
    if dropped:
        bad = lad[lad[needed].isna().any(axis=1)]
        for _, r in bad.iterrows():
            missing = [c for c in needed if pd.isna(r[c])]
            print(f"  dropped: {r['game_date']} batter={r['batter']} missing={missing}")

    usable = add_spray(usable, cfg)

    # spray sanity check: |spray| should be < ~50 deg, and distance vs spray plausible
    s = usable["spray_deg_pull"]
    print(f"\nSpray (pull-signed) distribution: min={s.min():.1f}  p25={s.quantile(.25):.1f}  "
          f"median={s.median():.1f}  p75={s.quantile(.75):.1f}  max={s.max():.1f} deg")
    print(f"Pull-side share (spray > 0): {100.0 * (s > 0).mean():.1f}%  "
          f"(HRs should skew heavily pull side)")
    corr = usable[["spray_deg_pull", "hit_distance_sc"]].corr().iloc[0, 1]
    print(f"corr(spray_pull, hit_distance_sc) = {corr:.3f}  "
          f"(mildly negative expected: oppo HRs need distance less often? pull HRs shorter fences)")

    # batter names (player_name in Statcast is the PITCHER)
    from pybaseball import playerid_reverse_lookup
    ids = usable["batter"].astype(int).unique().tolist()
    try:
        lookup = playerid_reverse_lookup(ids, key_type="mlbam")
        name_map = {
            int(r["key_mlbam"]): f"{r['name_first'].title()} {r['name_last'].title()}"
            for _, r in lookup.iterrows()
        }
    except Exception as e:
        print(f"WARNING — batter name lookup failed: {e}", file=sys.stderr)
        name_map = {}

    # per-play video ids (Savant clip links) + raw MP4s for inline playback
    print("\nFetching play video ids from the MLB Stats API ...")
    play_ids = fetch_play_ids(usable, cfg)
    print("Resolving Savant clips to MP4 urls ...")
    hr_play_ids = {k: v for k, v in play_ids.items()
                   if k in {(int(r.game_pk), int(r.at_bat_number)) for r in usable.itertuples()}}
    mp4s = fetch_video_mp4s(hr_play_ids, cfg, refresh=cfg.get("refresh_videos", False))
    no_video = 0

    # calibrate
    print("Calibrating drag coefficient against median hit_distance_sc ...")
    cd, d_sim, d_target, residual_pct = calibrate_cd(usable, cfg)
    print(f"  calibrated Cd = {cd:.4f}")
    print(f"  median simulated distance = {d_sim:.1f} ft")
    print(f"  median hit_distance_sc    = {d_target:.1f} ft")
    print(f"  CALIBRATION RESIDUAL      = {residual_pct:+.3f}%")
    if abs(residual_pct) > cfg["calib_tolerance_pct"]:
        raise RuntimeError(f"Calibration residual {residual_pct:.2f}% exceeds "
                           f"{cfg['calib_tolerance_pct']}% tolerance — not emitting output.")

    # Per-HR drag calibration: bisect each ball's Cd so its simulated landing
    # distance matches its own hit_distance_sc. The global Cd stays as the
    # sanity anchor and the fallback when a ball can't be matched.
    def calibrate_row_cd(r):
        target = r.hit_distance_sc
        lo, hi = 0.10, 0.80
        mid, d = cd, None
        for _ in range(30):
            mid = 0.5 * (lo + hi)
            d = landing_distance_ft(
                simulate(r.launch_speed, r.launch_angle, r.spray_deg_field, mid, cfg))
            if abs(d - target) < 0.5:
                return mid
            if d > target:
                lo = mid
            else:
                hi = mid
        return mid if d is not None and abs(d - target) < 15 else cd

    per_res = []
    records = []
    unmatched = 0
    ds = cfg["downsample_every"]
    rnd = cfg["coord_round_ft"]
    for r in usable.itertuples():
        row_cd = calibrate_row_cd(r)
        pts = simulate(r.launch_speed, r.launch_angle, r.spray_deg_field, row_cd, cfg)
        sim_d = landing_distance_ft(pts)
        if abs(sim_d - r.hit_distance_sc) > 1.5:
            unmatched += 1
        per_res.append(sim_d - r.hit_distance_sc)
        idx = list(range(0, len(pts), ds))
        if idx[-1] != len(pts) - 1:
            idx.append(len(pts) - 1)
        records.append({
            "game_date": str(pd.Timestamp(r.game_date).date()),
            "batter_name": name_map.get(int(r.batter), f"mlbam:{int(r.batter)}"),
            "pitcher_name": r.player_name,          # Statcast player_name = pitcher
            "batter": int(r.batter),
            "pitcher": int(r.pitcher),
            "pitch_type": None if pd.isna(r.pitch_type) else r.pitch_type,
            "release_speed": None if pd.isna(r.release_speed) else float(r.release_speed),
            "launch_speed": float(r.launch_speed),
            "launch_angle": float(r.launch_angle),
            "spray_deg_field": round(float(r.spray_deg_field), 2),
            "hit_distance_sc": float(r.hit_distance_sc),
            "sim_distance_ft": round(sim_d, 1),
            "stand": r.stand,
            "des": r.des,
            "home_team": r.home_team,
            "away_team": r.away_team,
            "hang_time_s": round((len(pts) - 1) * cfg["dt_s"], 2),
            "apex_ft": round(float(pts[:, 2].max()), 1),
            "postseason": r.game_type != "R",
            "game_type": r.game_type,
            "video_url": (f"https://baseballsavant.mlb.com/sporty-videos?playId="
                          f"{play_ids[(int(r.game_pk), int(r.at_bat_number))]}"
                          if (int(r.game_pk), int(r.at_bat_number)) in play_ids else None),
            "video_mp4": mp4s.get(play_ids.get((int(r.game_pk), int(r.at_bat_number)))),
            "points": [[round(float(x), rnd), round(float(y), rnd), round(float(z), rnd)]
                       for x, y, z in pts[idx]],
        })

    no_video = sum(1 for rec in records if rec["video_url"] is None)
    if no_video:
        print(f"WARNING — {no_video} HRs have no video playId", file=sys.stderr)

    per_res = np.array(per_res)
    print(f"\nPer-HR residual after per-ball Cd calibration (sim - statcast, ft): "
          f"mean={per_res.mean():+.2f}  std={per_res.std():.2f}  "
          f"max|res|={np.abs(per_res).max():.1f}")
    print(f"Balls not matchable within 1.5 ft (kept at global Cd): {unmatched}")

    out = {
        "meta": {
            "season": year,
            "team": "LAD",
            "pulled_range": [cfg["season_start"].isoformat(), cfg["season_end"].isoformat()],
            "failed_ranges": failed_ranges,
            "season_hr_total": season_hr_total,
            "regular_hr_total": reg_total,
            "postseason_hr_total": ps_total,
            "reconstructed": len(records),
            "dropped_missing_data": dropped,
            "calibrated_cd": round(cd, 4),
            "calibration_residual_pct": round(residual_pct, 3),
            "lift_cl": cfg["lift_cl"],
            "air_density_kgm3": cfg["air_density_kgm3"],
            "coordinate_system": "feet; origin=home plate; +x=center field; +y=right-field line; +z=up",
        },
        "trajectories": records,
    }
    cfg["out_path"].parent.mkdir(parents=True, exist_ok=True)
    tmp = cfg["out_path"].with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(out, f)
    tmp.rename(cfg["out_path"])
    size_mb = cfg["out_path"].stat().st_size / 1e6
    print(f"\nWrote {cfg['out_path']} ({size_mb:.1f} MB): "
          f"{len(records)}/{season_hr_total} HRs reconstructed "
          f"({dropped} dropped for missing measurement data)")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=date.today().year,
                    help="season to build (default: current year)")
    ap.add_argument("--refresh-videos", action="store_true",
                    help="HEAD-check cached clip urls, re-resolve dead ones")
    args = ap.parse_args()
    CONFIG["refresh_videos"] = args.refresh_videos
    main(args.year)
