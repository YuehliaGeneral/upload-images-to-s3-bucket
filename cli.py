#!/usr/bin/env python3
"""
CLI Configuration Module
Handles command line argument parsing and configuration management.
"""

import argparse
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


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
    fit_mode: str
    key_prefix: str
    key_suffix: str
    source_column: Optional[str]
    result_column: Optional[str]
    
    # Mode settings
    dry_run: bool
    overwrite_existing: bool
    backup_originals: bool
    backup_dir: str
    debug_save: bool
    debug_dir: str
    test_mode: bool
    test_rows: int
    for_delivero: bool = False


def get_config() -> Config:
    """Parse command line arguments and merge with config.py defaults."""
    load_dotenv(Path(__file__).with_name('.env'))
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
            FIT_MODE as default_fit_mode,
            KEY_PREFIX as default_key_prefix,
            KEY_SUFFIX as default_key_suffix,
            SOURCE_COLUMN as default_source_column,
            RESULT_COLUMN as default_result_column,
            DRY_RUN as default_dry_run,
            OVERWRITE_EXISTING as default_overwrite_existing,
            BACKUP_ORIGINALS as default_backup_originals,
            BACKUP_DIR as default_backup_dir,
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
        default_fit_mode = 'contain'
        default_key_prefix = ''
        default_key_suffix = ''
        default_source_column = None
        default_result_column = None
        default_dry_run = True
        default_overwrite_existing = False
        default_backup_originals = False
        default_backup_dir = 'original_image_backups'
        default_debug_save = False
        default_debug_dir = 'debug_images'
        default_test_mode = False
        default_test_rows = 5
        config_loaded = False
    
    mode_value = os.getenv('FOR_DELIVERO', 'false').strip().lower()
    if mode_value not in ('true', 'false'):
        parser.error('FOR_DELIVERO must be true or false')
    for_delivero = mode_value == 'true'
    deliveroo_settings = {
        'bucket_name': 'deliveroo-bucket-yjh5p6',
        'aws_region': 'ap-south-1',
        'target_width': 1200,
        'target_height': 800,
        'fit_mode': 'contain',
        'key_prefix': '',
        'key_suffix': '',
        'source_column': 'Thumbnail image',
        'result_column': 'Thumbnail image Deliveroo',
    }

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

    parser.add_argument('--fit-mode',
                       choices=['cover', 'contain'],
                       default=default_fit_mode,
                       help=(
                           "Image fit mode: 'cover' fills the target and center-crops; "
                           "'contain' fits the whole image on a white canvas "
                           f"(default: {default_fit_mode})"
                       ))

    parser.add_argument('--key-suffix',
                       default=default_key_suffix,
                       help=(
                           "Suffix to append before the generated .jpg extension, "
                           "for example -cover-1080x720 "
                           f"(default: {default_key_suffix!r})"
                       ))

    parser.add_argument('--key-prefix',
                       default=default_key_prefix,
                       help=(
                           "Root prefix for generated S3 keys. For wp-content URLs, "
                           "this replaces the wp-content root, for example "
                           "wp-content-snoonu "
                           f"(default: {default_key_prefix!r})"
                       ))

    parser.add_argument('--source-column',
                       default=default_source_column,
                       help=(
                           "CSV column to read image URLs from "
                           f"(default: auto-detect; config: {default_source_column!r})"
                       ))

    parser.add_argument('--result-column', '--destination-column',
                       dest='result_column',
                       default=default_result_column,
                       help=(
                           "CSV column to write uploaded S3 URLs into "
                           f"(default: auto-detect; config: {default_result_column!r})"
                       ))
    
    # Mode arguments
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--dry-run', action='store_true',
                           default=default_dry_run,
                           help='Simulate operations without uploading (safe mode)')
    mode_group.add_argument('--no-dry-run', '--upload', action='store_false',
                           dest='dry_run',
                           help='Actually upload images to S3 (PRODUCTION MODE)')

    overwrite_group = parser.add_mutually_exclusive_group()
    overwrite_group.add_argument('--overwrite-existing', action='store_true',
                                 default=default_overwrite_existing,
                                 help='Reprocess and upload even when the S3 object already exists')
    overwrite_group.add_argument('--skip-existing', action='store_false',
                                 dest='overwrite_existing',
                                 help='Skip S3 objects that already exist and are accessible')

    backup_group = parser.add_mutually_exclusive_group()
    backup_group.add_argument('--backup-originals', action='store_true',
                              default=default_backup_originals,
                              help='Save downloaded original images locally before processing')
    backup_group.add_argument('--no-backup-originals', action='store_false',
                              dest='backup_originals',
                              help='Do not save downloaded original images locally')

    parser.add_argument('--backup-dir',
                       default=default_backup_dir,
                       help=f'Directory for original image backups (default: {default_backup_dir})')
    
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
    
    if for_delivero:
        parser.set_defaults(
            **deliveroo_settings,
            input_csv='Delivero list - with thumbnails.csv',
            output_csv='deliveroo-processed.csv',
        )

    # Parse arguments
    args = parser.parse_args()
    if for_delivero:
        for name, expected in deliveroo_settings.items():
            if getattr(args, name) != expected:
                parser.error(
                    f'FOR_DELIVERO=true requires --{name.replace("_", "-")} '
                    f'to be {expected!r}; set FOR_DELIVERO=false for custom settings'
                )
    print(f'FOR_DELIVERO: {for_delivero}')
    
    # Print configuration source
    if config_loaded:
        print(f"✅ Configuration loaded from config.py (overridden by CLI args)")
    else:
        print(f"⚠️  config.py not found, using built-in defaults (overridden by CLI args)")
    
    # Print current settings
    print(f"⚠️  DRY_RUN mode: {args.dry_run}")
    print(f"🔁 OVERWRITE_EXISTING mode: {args.overwrite_existing}")
    print(f"🖼️  FIT_MODE: {args.fit_mode}")
    if args.key_prefix:
        print(f"📁 KEY_PREFIX: {args.key_prefix}")
    if args.key_suffix:
        print(f"🔑 KEY_SUFFIX: {args.key_suffix}")
    if args.source_column:
        print(f"📥 SOURCE_COLUMN: {args.source_column}")
    if args.result_column:
        print(f"📤 RESULT_COLUMN: {args.result_column}")
    if args.backup_originals:
        print(f"📦 BACKUP_ORIGINALS enabled: Will save originals to {args.backup_dir}")
    if args.test_mode:
        print(f"🧪 TEST_MODE enabled: Will process only {args.test_rows} rows")
    if args.debug_save:
        print(f"🐛 DEBUG_SAVE enabled: Will save images to {args.debug_dir}")
    
    # Return configuration object
    return Config(
        for_delivero=for_delivero,
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        bucket_name=args.bucket_name,
        aws_region=args.aws_region,
        target_width=args.target_width,
        target_height=args.target_height,
        fit_mode=args.fit_mode,
        key_prefix=args.key_prefix,
        key_suffix=args.key_suffix,
        source_column=args.source_column,
        result_column=args.result_column,
        dry_run=args.dry_run,
        overwrite_existing=args.overwrite_existing,
        backup_originals=args.backup_originals,
        backup_dir=args.backup_dir,
        debug_save=args.debug_save,
        debug_dir=args.debug_dir,
        test_mode=args.test_mode,
        test_rows=args.test_rows
    ) 
