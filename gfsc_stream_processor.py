#!/usr/bin/env python3
"""
GFSC Streaming Snow Probability Processor (S3 only)
===================================================

Streams GFSC V102 products straight from the CLMS HR-WSI S3 bucket into memory,
aggregates them across ALL requested years, and writes only the final
per-tile products. Nothing from S3 is stored on disk.

Per tile and period it writes (the first three have the same names/format as
the existing gfsc_snow_probability_processor.py "2_*_products" folders):

    {tile}_{month}[_h1|_h2]_snow_probability_all_years.tif   float32, %
    {tile}_{month}[_h1|_h2]_median_snow_all_years.tif        float32, % (median FSC)
    {tile}_{month}[_h1|_h2]_observation_count_all_years.tif  int16
    {tile}_{month}[_h1|_h2]_quality_fallback_all_years.tif   int16, 1 = fallback pixel

Output layout:
    gfsc_s3_results/2_bimonthly_products/   (default)
    gfsc_s3_results/2_monthly_products/

Masking (V102):
    GF == 205 (cloud), 255 (no data), 210 (water)  -> always invalid
    pixels with < MIN_VALID_OBS valid observations -> NoData

GF-QA == 3 ("minimal quality") observations are handled by --qa3-mode:
    fallback (default)  Strict first: QA=3 observations are ignored, exactly like
                        the existing processor. Only for pixels that then have
                        fewer than MIN_VALID_OBS observations, the result is
                        recomputed from ALL observations including QA=3. Those
                        pixels are marked 1 in the *_quality_fallback raster, and
                        observation_count shows the number of observations that
                        were actually used for each pixel.
    mask                Strict only (identical to the existing processor)
    keep                Use every observation regardless of QA (no flag raster)

Memory: one tile-month is held as a uint8 stack (days x years x H x W,
255 = invalid, QA=3 observations stored as value + 128 in fallback mode), so a
month is roughly 1 GB per tile. Tiles run one at a time.

Usage:
    python gfsc_stream_processor.py --tiles 35WPU            # quick test
    python gfsc_stream_processor.py                           # full run, bi-monthly
    python gfsc_stream_processor.py --resolution monthly
    python gfsc_stream_processor.py --years 2019,2023 --months 4
    python gfsc_stream_processor.py --qa3-mode mask           # strict, as the old processor
"""

import argparse
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import numpy as np
import rasterio
from botocore.config import Config
from rasterio.io import MemoryFile

# ============================================================================
# CONFIGURATION
# ============================================================================

YEARS_TO_PROCESS = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
MONTHS_TO_PROCESS = [3, 4, 5, 6]
TEMPORAL_RESOLUTION = "bimonthly"   # "bimonthly" (h1 = day 1-15, h2 = day 16-end) or "monthly"
TILES_TO_PROCESS = ["32VKL", "32VKM", "32VKN", "32VKP", "32VKQ", "32VLK", "32VLL", "32VLM", "32VLN", "32VLP", "32VLQ", "32VMK", "32VML", "32VMM", "32VMN", "32VMP", "32VMQ", "32VMR", "32VNK", "32VNL", "32VNM", "32VNN", "32VNP", "32VNQ", "32VNR", "32VPL", "32VPM", "32VPN", "32VPP", "32VPQ", "32VPR", "32WMS", "32WNS", "32WNT", "32WPA", "32WPS", "32WPT", "32WPU", "32WPV", "33WVM", "33WVN", "33WVP", "33WVQ", "33WVR", "33WVS", "33WWP", "33WWQ", "33WWR", "33WWS", "33WWT", "33WXR", "33WXS", "33WXT", "34WDA", "34WDB", "34WDC", "34WDD", "34WEB", "34WEC", "34WED", "34WFB", "34WFC", "34WFD", "35WMS", "35WMT", "35WMU", "35WNS", "35WNT", "35WNU", "35WPT", "35WPU"]

OUTPUT_DIR = "gfsc_s3_results"

# Minimum valid observations per pixel before a probability/median is reported
MIN_VALID_OBS = 5

# How GF-QA == 3 ("minimal quality") observations are used: "fallback", "mask" or
# "keep" (see module docstring).
QA3_MODE = "fallback"

# Parallel S3 fetches (I/O bound, so more threads than cores is fine)
FETCH_WORKERS = 16

# S3 access: same public HR-WSI endpoint/bucket/keys as gfsc_data_downloader.py.
# Override with environment variables if needed.
S3_ACCESS_KEY = os.environ.get("HRWSI_S3_ACCESS_KEY", "c4ae60af7b144053803c618a8860f7c9")
S3_SECRET_KEY = os.environ.get("HRWSI_S3_SECRET_KEY", "dcb3ba1f6eab45aaaec5802feef5e2e4")
S3_ENDPOINT_URL = "https://s3.WAW3-2.cloudferro.com"
S3_BUCKET = "HRWSI"

INVALID = 255  # sentinel in the uint8 stacks
QA3_OFFSET = 128  # fallback mode: QA=3 observations are stored as GF + 128

MONTH_NAMES = {1: 'january', 2: 'february', 3: 'march', 4: 'april', 5: 'may', 6: 'june',
               7: 'july', 8: 'august', 9: 'september', 10: 'october', 11: 'november', 12: 'december'}

PRODUCT_DATE = re.compile(r'_(\d{8})P7D_COMB_V102_(GF|GF-QA)\.tif$')


# ============================================================================
# S3 ACCESS
# ============================================================================

def make_client(workers: int):
    return boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        endpoint_url=S3_ENDPOINT_URL,
        config=Config(max_pool_connections=max(10, workers * 2),
                      retries={"max_attempts": 8, "mode": "standard"}),
    )


def list_products(client, tile: str, year: int, month: int) -> List[Dict]:
    """
    List complete (GF + GF-QA) V102 products for a tile/year/month.
    Key layout: GFSC/{tile}/{YYYY}/{MM}/{DD}/{product}/{product}_{GF|GF-QA}.tif
    """
    prefix = f"GFSC/{tile}/{year:04d}/{month:02d}/"
    keys: Dict[str, Dict[str, str]] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            m = PRODUCT_DATE.search(obj["Key"])
            if m:
                keys.setdefault(m.group(1), {})[m.group(2)] = obj["Key"]

    products = []
    for date_str, files in sorted(keys.items()):
        if "GF" in files and "GF-QA" in files:
            products.append({"date": datetime.strptime(date_str, "%Y%m%d"),
                             "gf_key": files["GF"], "qa_key": files["GF-QA"]})
    return products


def read_tif_bytes(client, key: str) -> Tuple[np.ndarray, Optional[dict]]:
    body = client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    with MemoryFile(body) as mem:
        with mem.open() as src:
            return src.read(1), {"transform": src.transform, "crs": src.crs}


def load_masked(client, product: Dict, mask_qa3: bool = True,
                encode_qa3: bool = False) -> Tuple[datetime, np.ndarray, dict]:
    """
    Fetch GF + GF-QA and return a uint8 array with INVALID where masked.
    mask_qa3: QA == 3 observations become INVALID.
    encode_qa3: instead keep them as GF + QA3_OFFSET so they can be used as a
    fallback later (takes precedence over mask_qa3).
    """
    gf, meta = read_tif_bytes(client, product["gf_key"])
    qa, _ = read_tif_bytes(client, product["qa_key"])

    out = gf.astype(np.uint8)  # GF values are 0-100 plus 205/210/255 flags
    out[(gf == 205) | (gf == 255) | (gf == 210)] = INVALID
    if encode_qa3:
        qa3 = (qa == 3) & (out <= 100)
        out[qa3] += QA3_OFFSET
    elif mask_qa3:
        out[qa == 3] = INVALID
    return product["date"], out, meta


# ============================================================================
# AGGREGATION
# ============================================================================

def aggregate(stack: np.ndarray, min_obs: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    stack: uint8 (N, H, W), INVALID = masked.
    Returns snow_probability (%), median_snow (%), observation_count (int).
    Median is exact and equals nanmedian over the valid values.
    """
    valid = stack != INVALID
    obs = valid.sum(axis=0).astype(np.int16)
    snow = ((stack > 0) & valid).sum(axis=0)

    with np.errstate(invalid="ignore", divide="ignore"):
        prob = snow / obs * 100.0
    prob = prob.astype(np.float32)

    # Exact median: INVALID (255) sorts last, so the valid values of each pixel
    # occupy the first obs[i] slots after sorting along the time axis.
    srt = np.sort(stack, axis=0)
    n = obs.astype(np.int64)
    lo = np.clip((n - 1) // 2, 0, None)[None]
    hi = np.clip(n // 2, 0, None)[None]
    lo_v = np.take_along_axis(srt, lo, axis=0)[0].astype(np.float32)
    hi_v = np.take_along_axis(srt, hi, axis=0)[0].astype(np.float32)
    median = (lo_v + hi_v) / 2.0
    del srt

    insufficient = obs < min_obs
    prob[insufficient] = np.nan
    median[insufficient] = np.nan
    return prob, median.astype(np.float32), obs


def aggregate_with_fallback(stack: np.ndarray, min_obs: int):
    """
    stack: uint8 (N, H, W), INVALID = masked, QA=3 observations stored as GF + 128.
    Strict result (QA=3 ignored) first; pixels that end up below min_obs are
    recomputed from all observations. Returns (prob, median, obs, fallback)
    where obs is the count actually used and fallback is int16 0/1.
    """
    is_qa3 = (stack != INVALID) & (stack >= QA3_OFFSET)
    all_obs = stack.copy()
    all_obs[is_qa3] -= QA3_OFFSET
    strict = stack.copy()
    strict[is_qa3] = INVALID
    del stack, is_qa3

    prob_s, med_s, obs_s = aggregate(strict, min_obs)
    del strict
    prob_a, med_a, obs_a = aggregate(all_obs, min_obs)
    del all_obs

    fallback = np.isnan(prob_s) & ~np.isnan(prob_a)
    prob = np.where(fallback, prob_a, prob_s).astype(np.float32)
    med = np.where(fallback, med_a, med_s).astype(np.float32)
    obs = np.where(fallback, obs_a, obs_s).astype(np.int16)
    return prob, med, obs, fallback.astype(np.int16)


def write_raster(data: np.ndarray, meta: dict, path: Path, description: str,
                 variable: str, dtype=rasterio.float32):
    path.parent.mkdir(parents=True, exist_ok=True)
    if dtype == rasterio.int16:
        nodata = -9999
        out = data.astype(np.int16)
    else:
        nodata = -9999.0
        out = data.astype(np.float32)
        out[np.isnan(data)] = nodata

    with rasterio.open(path, 'w', driver='GTiff', height=out.shape[0], width=out.shape[1],
                       count=1, dtype=dtype, crs=meta["crs"], transform=meta["transform"],
                       compress='lzw', nodata=nodata) as dst:
        dst.write(out, 1)
        dst.set_band_description(1, description)
        dst.update_tags(VARIABLE=variable, DESCRIPTION=description,
                        APPROACH='temporal_aggregation_all_years')


# ============================================================================
# DRIVER
# ============================================================================

def periods_for(resolution: str) -> List[Tuple[Optional[int], str, str]]:
    """(half, filename suffix, title suffix) for each output period of a month"""
    if resolution == "monthly":
        return [(None, "", "")]
    return [(1, "_h1", " 1-15"), (2, "_h2", " 16-end")]


def output_paths(out_dir: Path, tile: str, month: int, suffix: str,
                 with_flag: bool = False) -> Dict[str, Path]:
    base = f"{tile}_{MONTH_NAMES[month]}{suffix}"
    paths = {
        "snow_probability": out_dir / f"{base}_snow_probability_all_years.tif",
        "median_snow": out_dir / f"{base}_median_snow_all_years.tif",
        "observation_count": out_dir / f"{base}_observation_count_all_years.tif",
    }
    if with_flag:
        paths["quality_fallback"] = out_dir / f"{base}_quality_fallback_all_years.tif"
    return paths


def peak_memory_gb() -> Optional[float]:
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1e9
    except ImportError:
        return None


def process_tile_month(client, tile: str, month: int, years: List[int], resolution: str,
                       out_dir: Path, min_obs: int, workers: int, overwrite: bool,
                       qa3_mode: str = QA3_MODE) -> int:
    periods = periods_for(resolution)
    with_flag = qa3_mode == "fallback"
    todo = [p for p in periods
            if overwrite or not all(f.exists() for f in output_paths(out_dir, tile, month, p[1], with_flag).values())]
    if not todo:
        print(f"  {tile} {MONTH_NAMES[month]}: outputs exist, skipping")
        return 0

    t0 = time.time()
    products = []
    for year in years:
        found = list_products(client, tile, year, month)
        print(f"    {year}: {len(found)} products")
        products.extend(found)
    if not products:
        print(f"  {tile} {MONTH_NAMES[month]}: no products on S3")
        return 0

    # Fetch once per month; bi-monthly halves are split from the same data
    with ThreadPoolExecutor(max_workers=workers) as pool:
        loaded = list(pool.map(lambda p: load_masked(client, p, qa3_mode == "mask", qa3_mode == "fallback"), products))
    t_fetch = time.time() - t0
    meta = loaded[0][2]

    written = 0
    for half, suffix, title in todo:
        sel = [arr for date, arr, _ in loaded
               if half is None or (half == 1 and date.day <= 15) or (half == 2 and date.day >= 16)]
        label = f"{tile} {MONTH_NAMES[month].capitalize()}{title}"
        if not sel:
            print(f"  {label}: no observations")
            continue

        stack = np.stack(sel, axis=0)
        flag = None
        if with_flag:
            prob, median, obs, flag = aggregate_with_fallback(stack, min_obs)
        else:
            prob, median, obs = aggregate(stack, min_obs)
        del stack

        paths = output_paths(out_dir, tile, month, suffix, with_flag)
        write_raster(prob, meta, paths["snow_probability"],
                     f"Snow Probability (%) - {label}", 'snow_probability')
        write_raster(median, meta, paths["median_snow"],
                     f"Median Snow Cover (%) - {label}", 'median_snow_cover')
        write_raster(obs, meta, paths["observation_count"],
                     f"Observation Count - {label}", 'observation_count', dtype=rasterio.int16)
        if flag is not None:
            write_raster(flag, meta, paths["quality_fallback"],
                         f"Quality fallback (1 = QA=3 observations used) - {label}",
                         'quality_fallback', dtype=rasterio.int16)

        valid = ~np.isnan(prob)
        mean_p = float(np.mean(prob[valid])) if valid.any() else float('nan')
        fb_txt = f", {flag.mean() * 100:.1f}% pixels from QA=3 fallback" if flag is not None else ""
        print(f"  {label}: {len(sel)} obs, mean snow prob {mean_p:.1f}%, "
              f"{valid.mean() * 100:.0f}% pixels valid{fb_txt}")
        written += 1

    mem = peak_memory_gb()
    mem_txt = f", RSS {mem:.1f} GB" if mem is not None else ""
    print(f"  {tile} {MONTH_NAMES[month]}: {len(products)} products, fetch {t_fetch:.0f}s, "
          f"total {time.time() - t0:.0f}s{mem_txt}")
    return written


def parse_int_list(s: str) -> List[int]:
    return [int(x) for x in s.split(",") if x.strip()]


def main():
    ap = argparse.ArgumentParser(description="Streaming GFSC snow probability from S3")
    ap.add_argument("--tiles", help="comma-separated tiles (default: all configured)")
    ap.add_argument("--years", help="comma-separated years")
    ap.add_argument("--months", help="comma-separated months, e.g. 3,4,5,6")
    ap.add_argument("--resolution", choices=["bimonthly", "monthly"], default=TEMPORAL_RESOLUTION)
    ap.add_argument("--output-dir", default=OUTPUT_DIR)
    ap.add_argument("--workers", type=int, default=FETCH_WORKERS)
    ap.add_argument("--min-obs", type=int, default=MIN_VALID_OBS)
    ap.add_argument("--qa3-mode", choices=["fallback", "mask", "keep"], default=QA3_MODE,
                    help="GF-QA == 3 handling: fallback (strict, QA=3 only where too few obs; adds "
                         "a flag raster), mask (strict only), keep (use all)")
    ap.add_argument("--overwrite", action="store_true", help="recompute even if outputs exist")
    args = ap.parse_args()

    tiles = [t.strip().upper().lstrip("T") for t in args.tiles.split(",")] if args.tiles else TILES_TO_PROCESS
    years = parse_int_list(args.years) if args.years else YEARS_TO_PROCESS
    months = parse_int_list(args.months) if args.months else MONTHS_TO_PROCESS
    sub = "2_bimonthly_products" if args.resolution == "bimonthly" else "2_monthly_products"
    out_dir = Path(args.output_dir) / sub

    print("=" * 78)
    print("GFSC STREAMING PROCESSOR (S3 only, V102)")
    print("=" * 78)
    print(f"Tiles: {len(tiles)}   Years: {years}   Months: {months}")
    print(f"Resolution: {args.resolution}   Min obs: {args.min_obs}   Workers: {args.workers}   QA3 mode: {args.qa3_mode}")
    print(f"Output: {out_dir}")
    print("=" * 78)

    client = make_client(args.workers)
    t_all = time.time()
    for i, tile in enumerate(tiles, 1):
        print(f"\n[{i}/{len(tiles)}] Tile {tile}")
        t_tile = time.time()
        for month in months:
            try:
                process_tile_month(client, tile, month, years, args.resolution,
                                   out_dir, args.min_obs, args.workers, args.overwrite,
                                   args.qa3_mode)
            except Exception as e:
                print(f"  ERROR {tile} month {month}: {e}")
        print(f"  Tile {tile} done in {time.time() - t_tile:.0f}s")

    print(f"\nAll done in {(time.time() - t_all) / 60:.1f} min")


if __name__ == "__main__":
    main()
