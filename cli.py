#!/usr/bin/env python3
"""
CLI Configuration Module
Handles command line argument parsing and configuration management.
"""

import argparse
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration data class holding all application settings."""
    # File paths
    input_csv: str
    output_csv: str
    
    # AWS settings
    bucket_name: str
    aws_region: str
    
    # Image processing settings
    target_width: int
    target_height: int
    
    # Mode settings
    dry_run: bool
    debug_save: bool
    debug_dir: str
    test_mode: bool
    test_rows: int


def get_config() -> Config:
    """Parse command line arguments and merge with config.py defaults."""
    parser = argparse.ArgumentParser(
        description='Unified Image Processor - Check and upload images to S3',
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Load config.py defaults first
    try:
        from config import (
            INPUT_CSV as default_input_csv,
            OUTPUT_CSV as default_output_csv,
            BUCKET_NAME as default_bucket_name,
            AWS_REGION as default_aws_region,
            TARGET_WIDTH as default_target_width,
            TARGET_HEIGHT as default_target_height,
            DRY_RUN as default_dry_run,
            DEBUG_SAVE as default_debug_save,
            DEBUG_DIR as default_debug_dir,
            TEST_MODE as default_test_mode,
            TEST_ROWS as default_test_rows
        )
        config_loaded = True
    except ImportError:
        # Fallback defaults if config.py doesn't exist
        default_input_csv = 'yuhila - missing images.csv'
        default_output_csv = 'yuhila - missing images - processed.csv'
        default_bucket_name = 'deliveroo-bucket-yjh5p6'
        default_aws_region = 'ap-south-1'
        default_target_width = 1200
        default_target_height = 800
        default_dry_run = True
        default_debug_save = False
        default_debug_dir = 'debug_images'
        default_test_mode = False
        default_test_rows = 5
        config_loaded = False
    
    # Add command line arguments
    parser.add_argument('--input-csv', '--input', 
                       default=default_input_csv,
                       help=f'Input CSV file (default: {default_input_csv})')
    
    parser.add_argument('--output-csv', '--output',
                       default=default_output_csv,
                       help=f'Output CSV file (default: {default_output_csv})')
    
    parser.add_argument('--bucket-name', '--bucket',
                       default=default_bucket_name,
                       help=f'S3 bucket name (default: {default_bucket_name})')
    
    parser.add_argument('--aws-region', '--region',
                       default=default_aws_region,
                       help=f'AWS region (default: {default_aws_region})')
    
    parser.add_argument('--target-width', type=int,
                       default=default_target_width,
                       help=f'Target image width (default: {default_target_width})')
    
    parser.add_argument('--target-height', type=int,
                       default=default_target_height,
                       help=f'Target image height (default: {default_target_height})')
    
    # Mode arguments
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--dry-run', action='store_true',
                           default=default_dry_run,
                           help='Simulate operations without uploading (safe mode)')
    mode_group.add_argument('--no-dry-run', '--upload', action='store_false',
                           dest='dry_run',
                           help='Actually upload images to S3 (PRODUCTION MODE)')
    
    # Test mode arguments
    test_group = parser.add_mutually_exclusive_group()
    test_group.add_argument('--test-mode', action='store_true',
                           default=default_test_mode,
                           help='Process only first N rows for testing')
    test_group.add_argument('--no-test-mode', action='store_false',
                           dest='test_mode',
                           help='Process all rows')
    
    parser.add_argument('--test-rows', type=int,
                       default=default_test_rows,
                       help=f'Number of rows to process in test mode (default: {default_test_rows})')
    
    # Debug arguments
    debug_group = parser.add_mutually_exclusive_group()
    debug_group.add_argument('--debug-save', action='store_true',
                            default=default_debug_save,
                            help='Save processed images locally for debugging')
    debug_group.add_argument('--no-debug-save', action='store_false',
                            dest='debug_save',
                            help='Don\'t save processed images locally')
    
    parser.add_argument('--debug-dir',
                       default=default_debug_dir,
                       help=f'Directory for debug images (default: {default_debug_dir})')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Print configuration source
    if config_loaded:
        print(f"✅ Configuration loaded from config.py (overridden by CLI args)")
    else:
        print(f"⚠️  config.py not found, using built-in defaults (overridden by CLI args)")
    
    # Print current settings
    print(f"⚠️  DRY_RUN mode: {args.dry_run}")
    if args.test_mode:
        print(f"🧪 TEST_MODE enabled: Will process only {args.test_rows} rows")
    if args.debug_save:
        print(f"🐛 DEBUG_SAVE enabled: Will save images to {args.debug_dir}")
    
    # Return configuration object
    return Config(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        bucket_name=args.bucket_name,
        aws_region=args.aws_region,
        target_width=args.target_width,
        target_height=args.target_height,
        dry_run=args.dry_run,
        debug_save=args.debug_save,
        debug_dir=args.debug_dir,
        test_mode=args.test_mode,
        test_rows=args.test_rows
    ) 