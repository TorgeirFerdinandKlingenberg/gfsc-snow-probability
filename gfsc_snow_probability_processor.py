#!/usr/bin/env python3
"""
Complete GFSC Temporal Aggregation Processor
===========================================

This script processes GFSC data from both old format (2017-2024) and new format (2025)
using temporal aggregation approach - calculating snow probability for each tile
using ALL available years combined (like OpenEO approach).

Directory structure expected:
- gfsc_data/GFSC-wekeo/  (contains directories like GFSC_20170401-007_S1-S2_T32VMM_V101_1639994394/)
- gfsc_data/GFSC-s3/     (contains directories like CLMS_WSI_GFSC_060m_T32VML_20180401P7D_COMB_V102/)

Output structure (like OpenEO):
- 1_raw_yearly_data/        Individual yearly rasters
- 2_monthly_products/       Main results (temporal aggregation)
- 3_combined_products/      Summary tables

Usage:
1. Adjust the configuration section below
2. Run: python gfsc_snow_probability_processor.py
"""

import re
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
import seaborn as sns

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION - MODIFY THIS SECTION FOR YOUR NEEDS
# ============================================================================

# Data paths (adjust these to match your directory structure)
OLD_DATA_PATH = "gfsc_data/GFSC-wekeo"     # Path to WEkEO data (old format)
NEW_DATA_PATH = "gfsc_data/GFSC-s3"       # Path to S3 data (new format, reprocessed + 2025+)

# Processing parameters
YEARS_TO_PROCESS = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]  # All years
MONTHS_TO_PROCESS = [4, 5, 6]  # April, May, June
TILES_TO_PROCESS = ["32VKK", "32VKL", "32VKM", "32VKN", "32VKP", "32VKQ", "32VLJ", "32VLK", "32VLL", "32VLM", "32VLN", "32VLP", "32VLQ", "32VLR", "32VMJ", "32VMK", "32VML", "32VMM", "32VMN", "32VMP", "32VMQ", "32VMR", "32VNK", "32VNL", "32VNM", "32VNN", "32VNP", "32VNQ", "32VNR", "32VPL", "32VPM", "32VPN", "32VPP", "32VPQ", "32VPR", "32WMS", "32WNA", "32WNS", "32WNT", "32WNU", "32WNV", "32WPA", "32WPB", "32WPS", "32WPT", "32WPU", "32WPV", "33WVM", "33WVN", "33WVP", "33WVQ", "33WVR", "33WVS", "33WVT", "33WWP", "33WWQ", "33WWR", "33WWS", "33WWT", "33WWU", "33WXR", "33WXS", "33WXT", "33WXU", "34WDA", "34WDB", "34WDC", "34WDD", "34WEB", "34WEC", "34WED", "34WEE", "34WFB", "34WFC", "34WFD", "34WFE", "35WMS", "35WMT", "35WMU", "35WMV", "35WNS", "35WNT", "35WNU", "35WNV", "35WPS", "35WPT", "35WPU"]

# Output directory
OUTPUT_DIR = "gfsc_results"

# Quick test mode (set to True for faster testing with limited data)
QUICK_TEST = False

# ============================================================================
# UNIFIED GFSC PROCESSOR CLASS
# ============================================================================

class UnifiedGFSCProcessor:
    """
    Unified processor for WEkEO (old format) and S3 (new format) data
    with temporal aggregation approach (like OpenEO)
    """

    def __init__(self, old_data_path: str = "GFSC-wekeo", new_data_path: str = "GFSC-s3"):
        self.old_data_path = Path(old_data_path)
        self.new_data_path = Path(new_data_path)
        
    def scan_old_format_files(self, year: int, month: int, tile_id: str) -> List[Dict]:
        """
        Scan for old format files (2017-2024)
        Pattern: GFSC_YYYYMMDD-XXX_S1-S2_TILEXX_V101_TIMESTAMP
        """
        found_files = []
        
        if not self.old_data_path.exists():
            print(f"    Old data path does not exist: {self.old_data_path}")
            return found_files
            
        # Normalize tile_id (support both "32VKL" and "T32VKL")
        tile_id_pattern = tile_id if tile_id.upper().startswith('T') else f"T{tile_id}"
        
        # Pattern for old format directory names
        pattern = f"GFSC_{year:04d}{month:02d}\\d{{2}}-\\d{{3}}_S1-S2_{tile_id_pattern}_V101_\\d+"
        
        for item in self.old_data_path.iterdir():
            if item.is_dir() and re.match(pattern, item.name):
                # Extract date from directory name
                date_match = re.search(f'GFSC_({year:04d}{month:02d}\\d{{2}})-\\d+_S1-S2_{tile_id_pattern}', item.name)
                if date_match:
                    date_str = date_match.group(1)
                    try:
                        date = datetime.strptime(date_str, '%Y%m%d')
                        
                        # Look for GF and QC files
                        gf_file = item / f"{item.name}_GF.tif"
                        qc_file = item / f"{item.name}_QC.tif"
                        
                        if gf_file.exists() and qc_file.exists():
                            found_files.append({
                                'file_path': gf_file,
                                'qc_path': qc_file,
                                'tile_id': tile_id,
                                'date': date,
                                'year': year,
                                'month': month,
                                'format': 'old',
                                'product_dir': item
                            })
                    except ValueError:
                        continue
        
        return sorted(found_files, key=lambda x: x['date'])
    
    def scan_new_format_files(self, year: int, month: int, tile_id: str,
                               search_path: Path = None) -> List[Dict]:
        """
        Scan for new format files (CLMS_WSI_GFSC_060m_TILEXX_YYYYMMDDP7D_COMB_V102).
        Defaults to new_data_path but accepts an override for reprocessed data
        stored in old_data_path.
        """
        found_files = []
        path = search_path if search_path is not None else self.new_data_path

        if not path.exists():
            print(f"    Data path does not exist: {path}")
            return found_files

        # Normalize tile_id (support both "32VKL" and "T32VKL")
        tile_id_pattern = tile_id if tile_id.upper().startswith('T') else f"T{tile_id}"
        pattern = f"CLMS_WSI_GFSC_060m_{tile_id_pattern}_{year:04d}{month:02d}\\d{{2}}P7D_COMB_V102"

        for item in path.iterdir():
            if item.is_dir() and re.match(pattern, item.name):
                date_match = re.search(f'CLMS_WSI_GFSC_060m_{tile_id_pattern}_({year:04d}{month:02d}\\d{{2}})P7D_COMB_V102', item.name)
                if date_match:
                    date_str = date_match.group(1)
                    try:
                        date = datetime.strptime(date_str, '%Y%m%d')

                        gf_file = item / f"{item.name}_GF.tif"
                        qa_file = item / f"{item.name}_GF-QA.tif"

                        if gf_file.exists() and qa_file.exists():
                            found_files.append({
                                'file_path': gf_file,
                                'qc_path': qa_file,
                                'tile_id': tile_id,
                                'date': date,
                                'year': year,
                                'month': month,
                                'format': 'new',
                                'product_dir': item
                            })
                    except ValueError:
                        continue

        return sorted(found_files, key=lambda x: x['date'])

    def scan_all_files(self, year: int, month: int, tile_id: str) -> List[Dict]:
        """Scan for files in both old and new formats across all data paths"""
        old_files = self.scan_old_format_files(year, month, tile_id)
        # New format from GFSC-2025 (live data) and GFSC-2017-2024 (reprocessed data)
        new_files = self.scan_new_format_files(year, month, tile_id)
        new_files_reprocessed = self.scan_new_format_files(year, month, tile_id,
                                                            search_path=self.old_data_path)

        all_files = old_files + new_files + new_files_reprocessed
        return sorted(all_files, key=lambda x: x['date'])
    
    def load_gfsc_data(self, file_info: Dict) -> np.ndarray:
        """
        Load GFSC data with appropriate quality filtering based on format
        """
        gf_file = file_info['file_path']
        qc_file = file_info['qc_path']
        file_format = file_info['format']
        
        with rasterio.open(gf_file) as gf_src, rasterio.open(qc_file) as qc_src:
            gf_data = gf_src.read(1).astype(np.float32)
            qc_data = qc_src.read(1)
            
            # Basic filtering (always applied)
            gf_data[gf_data == 205] = np.nan  # clouds
            gf_data[gf_data == 255] = np.nan  # no data
            
            # Format-specific quality filtering
            if file_format == 'old':
                # Old format: QC values 0=high, 1=medium, 2=low, 3=minimal
                gf_data[qc_data == 3] = np.nan  # remove minimal quality only
            else:
                # New format: Different QA encoding
                gf_data[qc_data == 3] = np.nan  # adapt based on new QA specification
            
            return gf_data
    
    def save_temporal_aggregated_raster(self, data: np.ndarray, transform, crs, 
                                      filename: Path, description: str, 
                                      variable: str, dtype=rasterio.float32):
        """Save temporally aggregated raster"""
        
        filename.parent.mkdir(exist_ok=True)
        
        # Handle different data types
        if dtype == rasterio.int16:
            data_to_write = data.astype(np.int16)
            nodata_value = -9999
            data_to_write[np.isnan(data)] = nodata_value
        else:
            data_to_write = data.astype(np.float32)
            nodata_value = np.nan
        
        with rasterio.open(
            filename, 'w',
            driver='GTiff',
            height=data.shape[0],
            width=data.shape[1],
            count=1,
            dtype=dtype,
            crs=crs,
            transform=transform,
            compress='lzw',
            nodata=nodata_value
        ) as dst:
            dst.write(data_to_write, 1)
            dst.set_band_description(1, description)
            
            dst.update_tags(
                VARIABLE=variable,
                DESCRIPTION=description,
                APPROACH='temporal_aggregation_all_years'
            )
    
    def process_tile_temporal_aggregation(self, tile_id: str, month: int, years: List[int], output_dir: str) -> Dict:
        """
        Process one tile across all years for temporal aggregation (like OpenEO approach)
        """
        month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                      7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        month_name = month_names[month]
        
        print(f"\n=== Processing {tile_id} - {month_name} ===")
        print(f"Years: {years}")
        
        all_daily_data = []
        yearly_data = []
        reference_transform = None
        reference_crs = None
        
        # Collect data for each year
        for year in years:
            files = self.scan_all_files(year, month, tile_id)
            if len(files) > 0:
                print(f"  {year}: {len(files)} files")
                
                year_data = []
                for file_info in files:
                    try:
                        data = self.load_gfsc_data(file_info)
                        
                        # Get reference geospatial info from first file
                        if reference_transform is None:
                            with rasterio.open(file_info['file_path']) as src:
                                reference_transform = src.transform
                                reference_crs = src.crs
                        
                        all_daily_data.append(data)
                        year_data.append(data)
                        
                    except Exception as e:
                        print(f"    Error loading {file_info['date'].strftime('%Y-%m-%d')}: {e}")
                        continue
                
                if len(year_data) > 0:
                    yearly_data.append((year, year_data))
                    
                    # Save individual year data to raw folder
                    raw_dir = Path(output_dir) / "1_raw_yearly_data"
                    raw_dir.mkdir(exist_ok=True)
                    
                    year_stack = np.stack(year_data, axis=0)
                    year_prob = np.nanmean(year_stack > 0, axis=0) * 100
                    year_obs = np.sum(~np.isnan(year_stack), axis=0)
                    
                    # Save yearly probability
                    year_prob_file = raw_dir / f"{tile_id}_{year}_{month_name.lower()}_snow_probability.tif"
                    self.save_temporal_aggregated_raster(
                        year_prob, reference_transform, reference_crs,
                        year_prob_file, f"Snow Probability (%) - {tile_id} {month_name} {year}",
                        variable='snow_probability'
                    )
                    
                    # Save yearly observation count
                    year_obs_file = raw_dir / f"{tile_id}_{year}_{month_name.lower()}_observation_count.tif"
                    self.save_temporal_aggregated_raster(
                        year_obs, reference_transform, reference_crs,
                        year_obs_file, f"Observation Count - {tile_id} {month_name} {year}",
                        variable='observation_count', dtype=rasterio.int16
                    )
            else:
                print(f"  {year}: No files")
        
        print(f"  Total observations collected: {len(all_daily_data)}")
        
        if len(all_daily_data) == 0:
            print(f"  No data available for {tile_id} {month_name}")
            return None
        
        # Calculate temporal aggregation across ALL years
        print(f"  Calculating snow probability across all {len(all_daily_data)} observations...")
        snow_data_stack = np.stack(all_daily_data, axis=0)
        
        # Snow probability: percentage of days with snow > 0
        snow_presence = snow_data_stack > 0
        snow_probability = np.nanmean(snow_presence, axis=0) * 100
        
        # Observation count
        observation_count = np.sum(~np.isnan(snow_data_stack), axis=0)
        
        # Median snow cover
        median_snow_cover = np.nanmedian(snow_data_stack, axis=0)
        
        # Calculate statistics
        valid_prob = snow_probability[~np.isnan(snow_probability)]
        valid_obs = observation_count[~np.isnan(observation_count)]
        
        if len(valid_prob) > 0:
            result = {
                'tile_id': tile_id,
                'month': month,
                'month_name': month_name,
                'years_processed': years,
                'total_observations': len(all_daily_data),
                'snow_probability': snow_probability,
                'observation_count': observation_count,
                'median_snow_cover': median_snow_cover,
                'transform': reference_transform,
                'crs': reference_crs,
                'mean_snow_probability': np.mean(valid_prob),
                'median_snow_probability': np.median(valid_prob),
                'min_snow_probability': np.min(valid_prob),
                'max_snow_probability': np.max(valid_prob),
                'mean_observations_per_pixel': np.mean(valid_obs),
                'pixels_with_snow': np.sum(valid_prob > 0),
                'total_pixels': len(valid_prob),
                'snow_coverage_percent': (np.sum(valid_prob > 0) / len(valid_prob)) * 100
            }
            
            # Print statistics
            print(f"  Results for {tile_id} {month_name}:")
            print(f"    Mean snow probability: {np.mean(valid_prob):.1f}%")
            print(f"    Snow coverage: {result['snow_coverage_percent']:.1f}% of pixels")
            print(f"    Data quality: {np.mean(valid_obs):.1f} obs/pixel")
            
            return result
        else:
            print(f"  No valid data for {tile_id} {month_name}")
            return None
    
    def save_tile_temporal_csv(self, result: Dict, csv_file: Path):
        """Save individual tile temporal aggregation results"""
        
        csv_data = {
            'metric': [
                'tile_id', 'month', 'month_name', 'years_processed',
                'total_observations', 'total_pixels', 'mean_snow_probability',
                'median_snow_probability', 'min_snow_probability', 'max_snow_probability',
                'mean_observations_per_pixel', 'pixels_with_snow', 'snow_coverage_percent'
            ],
            'value': [
                result['tile_id'], result['month'], result['month_name'],
                f"{min(result['years_processed'])}-{max(result['years_processed'])}",
                result['total_observations'], result['total_pixels'],
                result['mean_snow_probability'], result['median_snow_probability'],
                result['min_snow_probability'], result['max_snow_probability'],
                result['mean_observations_per_pixel'], result['pixels_with_snow'],
                result['snow_coverage_percent']
            ]
        }
        
        df = pd.DataFrame(csv_data)
        df.to_csv(csv_file, index=False)
    
    def create_combined_tile_products(self, results: Dict, combined_dir: Path):
        """Create combined summary products across all tiles"""
        
        # Create summary table
        summary_data = []
        
        for month, month_data in results.items():
            month_names = {4: 'April', 5: 'May', 6: 'June'}
            month_name = month_names[month]
            
            for tile_id, result in month_data.items():
                summary_data.append({
                    'tile_id': result['tile_id'],
                    'month': result['month'],
                    'month_name': result['month_name'],
                    'total_observations': result['total_observations'],
                    'mean_snow_probability_percent': result['mean_snow_probability'],
                    'snow_coverage_percent': result['snow_coverage_percent'],
                    'mean_observations_per_pixel': result['mean_observations_per_pixel']
                })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_file = combined_dir / "temporal_aggregation_summary_all_tiles.csv"
            summary_df.to_csv(summary_file, index=False)
            
            print(f"Created combined summary: {summary_file.name}")
            
            # Print final summary
            print(f"\n=== FINAL SUMMARY BY TILE AND MONTH ===")
            for month in [4, 5, 6]:
                month_names = {4: 'April', 5: 'May', 6: 'June'}
                month_name = month_names[month]
                month_data = summary_df[summary_df['month'] == month]
                
                if len(month_data) > 0:
                    print(f"\n{month_name}:")
                    for _, row in month_data.iterrows():
                        print(f"  {row['tile_id']}: {row['mean_snow_probability_percent']:.1f}% mean probability, "
                              f"{row['snow_coverage_percent']:.1f}% coverage")
            
            return summary_df
        
        return None
    
    def process_all_tiles_temporal_aggregation(self, years: List[int] = None, months: List[int] = None, 
                                             tiles: List[str] = None, output_dir: str = ".") -> Dict:
        """
        Process all tiles with temporal aggregation - like OpenEO approach but for each tile
        """
        if years is None:
            years = list(range(2017, 2026))
        
        if months is None:
            months = [4, 5, 6]
        
        if tiles is None:
            tiles = ['T32VML', 'T32VMM', 'T32VNL', 'T32VNM']
        
        print("=== TEMPORAL AGGREGATION BY TILE (OpenEO Style) ===")
        print(f"Years: {years}")
        print(f"Months: {months}")
        print(f"Tiles: {tiles}")
        print(f"Approach: Calculate snow probability for each tile using ALL available years")
        
        # Create organized folder structure like OpenEO example
        output_path = Path(output_dir)
        raw_data_dir = output_path / "1_raw_yearly_data"
        monthly_products_dir = output_path / "2_monthly_products"
        combined_products_dir = output_path / "3_combined_products"
        
        raw_data_dir.mkdir(exist_ok=True)
        monthly_products_dir.mkdir(exist_ok=True)
        combined_products_dir.mkdir(exist_ok=True)
        
        print(f"\nResults will be saved in:")
        print(f"  Raw yearly data: {raw_data_dir}")
        print(f"  Monthly products: {monthly_products_dir}")
        print(f"  Combined products: {combined_products_dir}")
        
        results = {}
        processing_log = []
        start_time = time.time()
        
        # Process each month
        for month in months:
            month_names = {4: 'April', 5: 'May', 6: 'June'}
            month_name = month_names[month]
            
            print(f"\n{'='*60}")
            print(f"PROCESSING {month_name.upper()}")
            print(f"{'='*60}")
            
            results[month] = {}
            
            # Process each tile for this month
            for tile_id in tiles:
                result = self.process_tile_temporal_aggregation(tile_id, month, years, output_dir)
                
                if result:
                    results[month][tile_id] = result
                    
                    # Save monthly products for this tile
                    prob_file = monthly_products_dir / f"{tile_id}_{month_name.lower()}_snow_probability_all_years.tif"
                    obs_file = monthly_products_dir / f"{tile_id}_{month_name.lower()}_observation_count_all_years.tif"
                    median_file = monthly_products_dir / f"{tile_id}_{month_name.lower()}_median_snow_all_years.tif"
                    
                    self.save_temporal_aggregated_raster(
                        result['snow_probability'], result['transform'], result['crs'],
                        prob_file, f"Snow Probability (%) - {tile_id} {month_name} (All Years)",
                        variable='snow_probability'
                    )
                    
                    self.save_temporal_aggregated_raster(
                        result['observation_count'], result['transform'], result['crs'],
                        obs_file, f"Observation Count - {tile_id} {month_name} (All Years)",
                        variable='observation_count', dtype=rasterio.int16
                    )
                    
                    self.save_temporal_aggregated_raster(
                        result['median_snow_cover'], result['transform'], result['crs'],
                        median_file, f"Median Snow Cover (%) - {tile_id} {month_name} (All Years)",
                        variable='median_snow_cover'
                    )
                    
                    # Save CSV
                    csv_file = monthly_products_dir / f"{tile_id}_{month_name.lower()}_statistics_all_years.csv"
                    self.save_tile_temporal_csv(result, csv_file)
                    
                    result['files'] = {
                        'probability': prob_file,
                        'observations': obs_file,
                        'median': median_file,
                        'csv': csv_file
                    }
                    
                    processing_log.append(f"{tile_id} {month_name}: SUCCESS - {result['total_observations']} obs, "
                                        f"{result['snow_coverage_percent']:.1f}% snow coverage")
                    
                    print(f"    Saved: {prob_file.name}, {obs_file.name}, {csv_file.name}")
                else:
                    processing_log.append(f"{tile_id} {month_name}: FAILED - No data")
        
        # Create combined products (summary across all tiles)
        print(f"\n{'='*60}")
        print(f"CREATING COMBINED PRODUCTS")
        print(f"{'='*60}")
        
        self.create_combined_tile_products(results, combined_products_dir)
        
        # Final summary
        total_time = time.time() - start_time
        successful_datasets = sum(len(month_data) for month_data in results.values())
        total_possible = len(months) * len(tiles)
        
        print(f"\n{'='*60}")
        print(f"TEMPORAL AGGREGATION COMPLETE")
        print(f"{'='*60}")
        print(f"Total processing time: {total_time/60:.1f} minutes")
        print(f"Successfully processed: {successful_datasets}/{total_possible} tile-month combinations")
        
        # Save processing log
        log_file = output_path / "processing_log_temporal_aggregation.txt"
        with open(log_file, 'w') as f:
            f.write("Temporal Aggregation Processing Log\n")
            f.write("="*50 + "\n\n")
            f.write("Approach: OpenEO-style temporal aggregation for each tile\n")
            f.write("Snow probability calculated using ALL available years per tile\n\n")
            for entry in processing_log:
                f.write(entry + "\n")
        
        print(f"Saved processing log: {log_file}")
        
        return results

# ============================================================================
# ANALYSIS AND VISUALIZATION CLASS
# ============================================================================

class GFSCAnalyzer:
    """
    Analysis and visualization tools for temporal aggregation results
    """
    
    def __init__(self, results_dir: str = "."):
        self.results_dir = Path(results_dir)
        
    def create_snow_probability_plots(self, results: Dict, save_plot: bool = True):
        """
        Create snow probability visualization plots for temporal aggregation results
        """
        month_names = {4: 'April', 5: 'May', 6: 'June'}
        
        for month, month_data in results.items():
            month_name = month_names[month]
            
            # Create subplot for all tiles
            n_tiles = len(month_data)
            if n_tiles == 0:
                continue
                
            cols = 2
            rows = (n_tiles + 1) // 2
            
            fig, axes = plt.subplots(rows, cols, figsize=(12, 6*rows))
            if n_tiles == 1:
                axes = [axes]
            elif rows == 1:
                axes = axes.reshape(1, -1)
            
            tile_idx = 0
            for tile_id, result in month_data.items():
                row = tile_idx // cols
                col = tile_idx % cols
                ax = axes[row, col] if rows > 1 else axes[col]
                
                probability = result['snow_probability']
                
                # Plot snow probability
                im = ax.imshow(probability, cmap='Blues', vmin=0, vmax=100)
                ax.set_title(f'{tile_id} - {month_name} (All Years)\nSnow Probability')
                ax.axis('off')
                
                # Add colorbar
                plt.colorbar(im, ax=ax, label='Snow Probability (%)', shrink=0.8)
                
                # Add statistics as text
                valid_prob = probability[~np.isnan(probability)]
                if len(valid_prob) > 0:
                    pixels_with_snow = np.sum(valid_prob > 0)
                    total_pixels = len(valid_prob)
                    snow_percentage = (pixels_with_snow / total_pixels) * 100
                    mean_prob = np.mean(valid_prob)
                    
                    stats_text = f'Pixels with snow: {pixels_with_snow:,}/{total_pixels:,} ({snow_percentage:.1f}%)\nMean probability: {mean_prob:.1f}%'
                    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
                           verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                           fontsize=9)
                
                tile_idx += 1
            
            # Hide empty subplots
            for i in range(tile_idx, rows * cols):
                row = i // cols
                col = i % cols
                ax = axes[row, col] if rows > 1 else axes[col]
                ax.axis('off')
            
            plt.suptitle(f'Snow Probability - {month_name} (Temporal Aggregation All Years)', fontsize=16)
            plt.tight_layout()
            
            if save_plot:
                plot_file = self.results_dir / f"snow_probability_{month_name.lower()}_all_tiles.png"
                plt.savefig(plot_file, dpi=300, bbox_inches='tight')
                print(f"Saved plot: {plot_file}")
            
            # plt.show() # Uncomment to display plots interactively

# ============================================================================
# MAIN PROCESSING FUNCTIONS
# ============================================================================

def check_data_availability():
    """Check what data is available in your directories"""
    print("=== Checking Data Availability ===\n")
    
    old_path = Path(OLD_DATA_PATH)
    new_path = Path(NEW_DATA_PATH)
    
    # Check old format data
    if old_path.exists():
        old_dirs = [d for d in old_path.iterdir() if d.is_dir()]
        print(f"Old format directory: {old_path}")
        print(f"Found {len(old_dirs)} subdirectories")
        
        if old_dirs:
            print("Sample old format directories:")
            for d in old_dirs[:3]:
                print(f"  {d.name}")
                gf_files = list(d.glob("*_GF.tif"))
                qc_files = list(d.glob("*_QC.tif"))
                print(f"    GF files: {len(gf_files)}, QC files: {len(qc_files)}")
        print()
    else:
        print(f"Old format directory not found: {old_path}\n")
    
    # Check new format data  
    if new_path.exists():
        new_dirs = [d for d in new_path.iterdir() if d.is_dir()]
        print(f"New format directory: {new_path}")
        print(f"Found {len(new_dirs)} subdirectories")
        
        if new_dirs:
            print("Sample new format directories:")
            for d in new_dirs[:3]:
                print(f"  {d.name}")
                gf_files = list(d.glob("*_GF.tif"))
                qa_files = list(d.glob("*_GF-QA.tif"))
                print(f"    GF files: {len(gf_files)}, QA files: {len(qa_files)}")
        print()
    else:
        print(f"New format directory not found: {new_path}\n")

def run_quick_test():
    """Run a quick test with limited data"""
    print("=== Running Quick Test ===\n")
    
    # Initialize processor
    processor = UnifiedGFSCProcessor(
        old_data_path=OLD_DATA_PATH,
        new_data_path=NEW_DATA_PATH
    )
    
    # Test with limited data
    test_years = [2017, 2025] if 2025 in YEARS_TO_PROCESS else [2017]
    test_months = [4]  # Just April
    test_tiles = [TILES_TO_PROCESS[0]]  # Just first tile
    
    print(f"Testing with years: {test_years}")
    print(f"Testing with months: {test_months}")
    print(f"Testing with tiles: {test_tiles}")
    print()
    
    # Create output directory
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    
    # Process test data
    results = processor.process_all_tiles_temporal_aggregation(
        years=test_years,
        months=test_months,
        tiles=test_tiles,
        output_dir=OUTPUT_DIR
    )
    
    if results:
        print("\nQuick test successful!")
        print("You can now run the full processing by setting QUICK_TEST = False")
        
        # Show visualization
        analyzer = GFSCAnalyzer(OUTPUT_DIR)
        analyzer.create_snow_probability_plots(results)
        
    else:
        print("\nQuick test failed - check your data paths and format")
    
    return results

def run_full_processing():
    """Run full processing with temporal aggregation by tile (OpenEO style)"""
    print("=== Running Full Processing - TEMPORAL AGGREGATION BY TILE ===\n")
    print("Approach: OpenEO-style temporal aggregation for each tile individually")
    print("Each tile gets snow probability calculated using ALL available years\n")
    
    # Create output directory
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    
    # Initialize processor
    processor = UnifiedGFSCProcessor(
        old_data_path=OLD_DATA_PATH,
        new_data_path=NEW_DATA_PATH
    )
    
    print(f"Processing years: {YEARS_TO_PROCESS}")
    print(f"Processing months: {MONTHS_TO_PROCESS}")
    print(f"Processing tiles: {TILES_TO_PROCESS} (individually)")
    print(f"Output directory: {OUTPUT_DIR}")
    print()
    
    # Process all tiles with temporal aggregation
    results = processor.process_all_tiles_temporal_aggregation(
        years=YEARS_TO_PROCESS,
        months=MONTHS_TO_PROCESS,
        tiles=TILES_TO_PROCESS,
        output_dir=OUTPUT_DIR
    )
    
    if results:
        print(f"\nTemporal aggregation by tile processing complete!")
        print(f"Check the '{OUTPUT_DIR}' directory structure:")
        print(f"  📁 1_raw_yearly_data/ - Individual yearly rasters for each tile")
        print(f"  📁 2_monthly_products/ - Monthly snow probability rasters (all years combined)")
        print(f"  📁 3_combined_products/ - Summary tables and statistics")
        print(f"  📄 processing_log_temporal_aggregation.txt")
        
        # Create visualizations
        print(f"\nCreating visualizations...")
        analyzer = GFSCAnalyzer(OUTPUT_DIR)
        analyzer.create_snow_probability_plots(results)
        
        # Print final guidance
        print(f"\n=== USAGE GUIDE ===")
        print(f"Main products in 2_monthly_products/:")
        print(f"  - {TILES_TO_PROCESS[0]}_april_snow_probability_all_years.tif")
        print(f"  - {TILES_TO_PROCESS[0]}_may_snow_probability_all_years.tif") 
        print(f"  - {TILES_TO_PROCESS[0]}_june_snow_probability_all_years.tif")
        print(f"  - (repeated for each tile)")
        print(f"\nSnow Probability Values (0-100%):")
        print(f"  0-20%:  Low risk for snow")
        print(f"  20-40%: Moderate risk")
        print(f"  40-60%: High risk")
        print(f"  60%+:   Very high risk")
        
    else:
        print(f"\nTemporal aggregation processing failed - check your data and configuration")
    
    return results

def print_usage_instructions():
    """Print detailed usage instructions"""
    print("""
=== GFSC Temporal Aggregation Processor Usage Instructions ===

1. DATA PREPARATION:
   - Ensure your data is organized in the expected directory structure:
     * gfsc_data/GFSC-wekeo/ containing old format directories (WEkEO downloads)
     * gfsc_data/GFSC-s3/ containing new format directories (S3 downloads, reprocessed + 2025+)

2. CONFIGURATION:
   - Edit the configuration section at the top of this script
   - Adjust paths, years, months, and tiles as needed

3. PROCESSING OPTIONS:
   - Quick test: Set QUICK_TEST = True (recommended first)
   - Full processing: Set QUICK_TEST = False

4. OUTPUT STRUCTURE (like OpenEO):
   - 1_raw_yearly_data/: Individual yearly rasters
   - 2_monthly_products/: Main results (temporal aggregation)
   - 3_combined_products/: Summary tables

5. MAIN PRODUCTS:
   Each tile gets snow probability calculated using ALL years combined:
   - {tile}_april_snow_probability_all_years.tif
   - {tile}_may_snow_probability_all_years.tif  
   - {tile}_june_snow_probability_all_years.tif

6. INTERPRETATION:
   Snow probability = (days with snow > 0) / (total valid days)
   Values range from 0-100% representing frequency of snow occurrence

For questions or issues, check the processing log files created during execution.
""")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("GFSC Temporal Aggregation Processor")
    print("=" * 50)
    
    # First, check data availability
    check_data_availability()
    
    # Show usage instructions
    print_usage_instructions()
    
    # Ask user what to do
    if QUICK_TEST:
        response = input("Run quick test? (y/n): ").lower().strip()
        if response in ['y', 'yes']:
            results = run_quick_test()
        else:
            print("Skipping processing. Edit the script configuration and run again.")
    else:
        response = input("Run full processing? This may take a long time. (y/n): ").lower().strip()
        if response in ['y', 'yes']:
            results = run_full_processing()
        else:
            print("Skipping processing. Set QUICK_TEST = True for testing first.")


    print("\nDone!")
