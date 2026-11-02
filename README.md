# GFSC Snow Probability Tools

Tools for downloading and processing Gap-filled Fractional Snow Cover (GFSC) data from the Copernicus Land Monitoring Service (CLMS) to compute snow probability.

## What is GFSC?

[Gap-filled Fractional Snow Cover](https://land.copernicus.eu/en/products/snow/high-resolution-gap-filled-fractional-snow-cover) is a daily snow cover product at 60 m resolution that combines Sentinel-1 (radar) and Sentinel-2 (optical) data for robust snow detection with gap-filling.

## Contents

| File | Description |
|------|-------------|
| `gfsc_stream_processor.py` | **Recommended.** Streams GFSC V102 directly from S3 into memory and writes only the finished snow probability products (no download to disk) |
| `gfsc_data_downloader.py` | Downloads GFSC data from CDSE, WEkEO and S3 (including reprocessed years) to disk |
| `gfsc_snow_probability_processor.py` | Computes snow probability from data downloaded to disk |
| `MGRS_tiles.gpkg` | MGRS tile footprints, used by the downloader for spatial filtering |

## Quick start

### 1. Install dependencies

```bash
pip install rasterio numpy pandas matplotlib seaborn hda boto3 retry tqdm geopandas shapely pyproj pyogrio python-dotenv
```

The streaming processor only needs `boto3`, `rasterio` and `numpy` (plus `psutil` if you want to see memory usage).

### 2a. Stream from S3 (recommended, V102 only)

```bash
python gfsc_stream_processor.py --tiles 35WPU    # quick test
python gfsc_stream_processor.py                   # all tiles, March-June, bi-monthly
```

Results are written to `gfsc_s3_results/2_bimonthly_products/` (or `2_monthly_products/` with `--resolution monthly`), named `{tile}_{month}_{h1|h2}_{product}_all_years.tif` (h1 = days 1-15, h2 = day 16 to end of month). All years 2017-2026 are combined per tile. Grid: 1830 x 1830 pixels, 60 m, in the tile's CRS. NoData is -9999.

| Product | Type | Meaning |
|---|---|---|
| `snow_probability` | float32, % | Share of valid observations (all years) with snow cover > 0 |
| `median_snow` | float32, % | Median fractional snow cover over the valid observations |
| `observation_count` | int16 | Number of observations actually used for the pixel |
| `quality_fallback` | int16, 0/1 | 1 = pixel is based on QA=3 observations (see below) |

Pixels with fewer than 5 valid observations (`--min-obs`) are NoData in `snow_probability` and `median_snow`.

**Masking.** GF = 205 (cloud / cloud shadow), 210 (water) and 255 (no data) are always invalid. Observations with quality class `GF-QA = 3` ("minimal quality") are handled by `--qa3-mode`:

- `fallback` (default): strict first, QA=3 is ignored. Only for pixels left with fewer than 5 observations is the result recomputed from all observations including QA=3, and `quality_fallback` is set to 1.
- `mask`: strict only, same as `gfsc_snow_probability_processor.py` (no `quality_fallback` file)
- `keep`: use all observations

**Known limitations.**

- V102 marks a lot of sea and some coast as no data, so coastal and border tiles have few valid pixels.
- Pixels with `quality_fallback = 1` largely coincide with forest. During the snow season they show lower snow probability than nearby unflagged pixels, most strongly in south-east Norway early in the season (up to about 25 percentage points lower in the first half of March), and the difference fades out towards June. It is not resolved whether the product under-detects snow under canopy or whether forest really holds less snow. Treat flagged pixels separately, or as low-confidence, for quantitative use.

### 2b. Download data to disk

Copy `.env.example` to `.env`, fill in your credentials, and run:

```bash
python gfsc_data_downloader.py
```

### 3. Process downloaded data

```bash
python gfsc_snow_probability_processor.py
```

Note that data from WEkEO (V101, 2019-2024 in older downloads) and S3 (V102) are different product versions with different masking of water and missing data, so the results are not directly comparable. If the same tile and day exist in both formats, `gfsc_snow_probability_processor.py` counts it twice.

### Data availability

S3 has all years 2017-2026 with roughly 94-98 % coverage per year (March-June, 71 tiles). CDSE has the same data as S3 for 2017, 2025 and 2026, is slightly lower for 2019 and 2021 (about 93 %), and is missing almost everything for 2023-2024 (15 % and 0 %). The remaining gaps exist in both sources and appear to be data that was never produced.

## Documentation

See [Gap-filled Fractional Snow Cover](https://land.copernicus.eu/en/products/snow/high-resolution-gap-filled-fractional-snow-cover) for details on the GFSC product.

## Credentials

- **S3** (HR-WSI, all years 2017-2026): No registration needed. The streaming processor uses S3 only.
- **CDSE** (optional): Register for free at [CDSE](https://dataspace.copernicus.eu/) and put `CDSE_USER` and `CDSE_PASSWORD` in `.env`. Searching does not require login, only downloading does.
- **WEkEO** (older V101 data that has not been reprocessed): Register at [WEkEO](https://wekeo.copernicus.eu/register/) and put your credentials in `.env` (see `.env.example`)

## License

MIT License - see [LICENSE](LICENSE)

The S3 access in `gfsc_data_downloader.py` and `gfsc_stream_processor.py` (endpoint, bucket and key structure) is based on [eea/clms-hrwsi-api-client-python](https://github.com/eea/clms-hrwsi-api-client-python).
