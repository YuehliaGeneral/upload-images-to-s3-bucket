#!/usr/bin/env python3
"""
Unified Image Processor
Checks for missing images in S3 bucket and uploads them if they don't exist.
Combines functionality from check_images.py and s3_image_processor.py
"""

import os
import pandas as pd
from PIL import Image
import requests
from io import BytesIO
import boto3
import logging
from datetime import datetime
from tqdm import tqdm
from urllib.parse import urlparse
from typing import Optional, Tuple
import argparse

from dotenv import load_dotenv
load_dotenv()

def parse_arguments():
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
    
    return args

# --- CONFIGURATION ---
config = parse_arguments()

# Set global variables from config
INPUT_CSV = config.input_csv
OUTPUT_CSV = config.output_csv
BUCKET_NAME = config.bucket_name
AWS_REGION = config.aws_region
TARGET_WIDTH = config.target_width
TARGET_HEIGHT = config.target_height
DRY_RUN = config.dry_run
DEBUG_SAVE = config.debug_save
DEBUG_DIR = config.debug_dir
TEST_MODE = config.test_mode
TEST_ROWS = config.test_rows

# --- LOGGING SETUP ---
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = datetime.now().strftime("unified_processor_%Y%m%d_%H%M%S.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, log_filename)),
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)

# Create debug directory if needed
if DEBUG_SAVE:
    os.makedirs(DEBUG_DIR, exist_ok=True)

# --- AWS S3 CLIENT ---
try:
    s3 = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    logger.info("S3 client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize S3 client: {e}")
    raise


def load_csv_with_encoding(file_path: str) -> pd.DataFrame:
    """Load CSV with robust encoding handling."""
    encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            logger.info(f"Successfully loaded CSV using {encoding} encoding")
            return df
        except UnicodeDecodeError:
            continue
    
    raise ValueError(f"Could not read {file_path} with any of the tried encodings: {encodings_to_try}")


def extract_s3_path(url: str) -> str:
    """Extract S3 path from URL."""
    if pd.isna(url) or not str(url).strip():
        raise ValueError("Empty or invalid URL")
    
    parsed = urlparse(str(url))
    return parsed.path.lstrip('/')


def generate_s3_key(original_url: str) -> str:
    """Generate S3 key from original URL, converting to .jpg format."""
    try:
        # First try the wp-content approach from s3_image_processor.py
        path_start = original_url.find('/wp-content/')
        if path_start != -1:
            key_path = original_url[path_start:]
            base, _ = os.path.splitext(key_path)
            return (base.lstrip('/') + '.jpg')
        
        # Fallback to full path approach
        path = extract_s3_path(original_url)
        base, ext = os.path.splitext(path)
        # Convert to .jpg if it's an image extension
        if ext.lower() in ['.png', '.jpeg', '.jpg', '.gif', '.webp']:
            return base + '.jpg'
        return path
        
    except Exception as e:
        logger.error(f"Failed to generate S3 key for URL {original_url}: {e}")
        raise


def check_s3_object_exists_and_accessible(s3_key: str) -> Tuple[bool, int, str]:
    """Check if object exists in S3 bucket and is accessible.
    
    Returns:
        Tuple of (needs_upload, status_code, status_message)
    """
    try:
        # First check if object exists
        s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        
        # If exists, test actual accessibility by trying to access the URL
        s3_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        response = requests.head(s3_url, timeout=10)
        
        if response.status_code == 200:
            return False, 200, "EXISTS_ACCESSIBLE"
        elif response.status_code == 403:
            logger.warning(f"S3 object {s3_key} exists but returns 403 - will re-upload")
            return True, 403, "EXISTS_403_REUPLOAD"
        else:
            logger.warning(f"S3 object {s3_key} exists but returns {response.status_code} - will re-upload")
            return True, response.status_code, f"EXISTS_{response.status_code}_REUPLOAD"
            
    except s3.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            return True, 404, "NOT_EXISTS"
        else:
            logger.error(f"Unexpected S3 error for key {s3_key}: {e}")
            return True, error_code, f"S3_ERROR_{error_code}"
    except requests.RequestException as e:
        logger.error(f"Error testing accessibility of {s3_key}: {e}")
        return True, 0, f"ACCESS_TEST_ERROR"
    except Exception as e:
        logger.error(f"Error checking S3 object {s3_key}: {e}")
        return True, 0, f"UNKNOWN_ERROR"


def center_crop_image(img: Image.Image, target_width: int, target_height: int) -> Image.Image:
    """Resize image to fit within target dimensions with white background, no crop."""
    img_ratio = img.width / img.height
    target_ratio = target_width / target_height

    if img_ratio > target_ratio:
        new_width = target_width
        new_height = int(target_width / img_ratio)
    else:
        new_height = target_height
        new_width = int(target_height * img_ratio)

    resized_img = img.resize((new_width, new_height), Image.LANCZOS)
    new_img = Image.new("RGB", (target_width, target_height), (255, 255, 255))
    paste_x = (target_width - new_width) // 2
    paste_y = (target_height - new_height) // 2
    new_img.paste(resized_img, (paste_x, paste_y))
    return new_img


def process_image_from_url(url: str, debug_filename: Optional[str] = None) -> BytesIO:
    """Download and process image from URL."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        img = Image.open(BytesIO(response.content)).convert('RGB')
        img = center_crop_image(img, TARGET_WIDTH, TARGET_HEIGHT)

        if DEBUG_SAVE and debug_filename:
            debug_path = os.path.join(DEBUG_DIR, debug_filename)
            img.save(debug_path, format='JPEG', quality=85)
            logger.debug(f"Debug image saved: {debug_path}")

        buffer = BytesIO()
        img.save(buffer, format='JPEG', quality=85)
        buffer.seek(0)
        return buffer
        
    except Exception as e:
        logger.error(f"Failed to process image from URL {url}: {e}")
        raise


def upload_to_s3(image_buffer: BytesIO, s3_key: str) -> str:
    """Upload image buffer to S3."""
    try:
        s3.upload_fileobj(
            image_buffer,
            BUCKET_NAME,
            s3_key,
            ExtraArgs={'ContentType': 'image/jpeg', 'ACL': 'public-read'}
        )
        return "UPLOADED"
    except Exception as e:
        logger.error(f"Failed to upload to S3 key {s3_key}: {e}")
        raise


def process_single_image(url: str, row_index: int) -> Tuple[str, str, str, int]:
    """Process a single image: check existence/accessibility and upload if needed.
    
    Returns:
        Tuple of (s3_key, status, s3_url, response_code)
    """
    try:
        # Generate S3 key
        s3_key = generate_s3_key(url)
        s3_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        
        # Check if exists and is accessible
        needs_upload, status_code, check_status = check_s3_object_exists_and_accessible(s3_key)
        
        if not needs_upload:
            logger.info(f"Row {row_index}: Image exists and accessible (HTTP {status_code}) - {s3_key}")
            return s3_key, "EXISTS_OK", s3_url, status_code
        
        # Log why we need to upload
        if check_status.startswith("EXISTS_403"):
            logger.info(f"Row {row_index}: Image exists but 403 forbidden (HTTP {status_code}) - will re-upload - {s3_key}")
        elif check_status.startswith("EXISTS_"):
            logger.info(f"Row {row_index}: Image exists but HTTP {status_code} - will re-upload - {s3_key}")
        elif check_status == "NOT_EXISTS":
            logger.info(f"Row {row_index}: Image not found (HTTP 404) - will upload - {s3_key}")
        else:
            logger.info(f"Row {row_index}: {check_status} - will upload - {s3_key}")
        
        if DRY_RUN:
            logger.info(f"Row {row_index}: [DRY RUN] Would upload - {s3_key}")
            return s3_key, f"WOULD_UPLOAD_{check_status}", s3_url, status_code
        
        # Process and upload image
        debug_filename = os.path.basename(s3_key) if DEBUG_SAVE else None
        image_buffer = process_image_from_url(url, debug_filename)
        upload_result = upload_to_s3(image_buffer, s3_key)
        
        # Verify upload worked by checking again
        verify_needs_upload, verify_status_code, verify_status = check_s3_object_exists_and_accessible(s3_key)
        if not verify_needs_upload and verify_status_code == 200:
            logger.info(f"Row {row_index}: Successfully uploaded and verified (HTTP {verify_status_code}) - {s3_key}")
            return s3_key, "UPLOADED_OK", s3_url, verify_status_code
        else:
            logger.warning(f"Row {row_index}: Upload completed but verification failed (HTTP {verify_status_code}) - {s3_key}")
            return s3_key, f"UPLOADED_VERIFY_FAIL_{verify_status_code}", s3_url, verify_status_code
        
    except Exception as e:
        logger.error(f"Row {row_index}: Failed to process {url} - {e}")
        return "", f"ERROR: {str(e)}", "", 0


def main():
    """Main processing function."""
    logger.info("="*60)
    logger.info("UNIFIED IMAGE PROCESSOR STARTED")
    logger.info("="*60)
    logger.info(f"Input CSV: {INPUT_CSV}")
    logger.info(f"Output CSV: {OUTPUT_CSV}")
    logger.info(f"S3 Bucket: {BUCKET_NAME}")
    logger.info(f"Target Dimensions: {TARGET_WIDTH}x{TARGET_HEIGHT}")
    logger.info(f"Dry Run Mode: {DRY_RUN}")
    logger.info(f"Debug Save: {DEBUG_SAVE}")
    logger.info(f"Test Mode: {TEST_MODE}")
    if TEST_MODE:
        logger.info(f"Test Rows: {TEST_ROWS}")
    
    # Safety confirmation for production runs
    if not DRY_RUN and not TEST_MODE:
        print("\n" + "="*60)
        print("⚠️  WARNING: PRODUCTION MODE ENABLED")
        print("="*60)
        print("This will ACTUALLY UPLOAD images to S3!")
        print(f"Bucket: {BUCKET_NAME}")
        print(f"CSV file: {INPUT_CSV}")
        print("This action cannot be undone.")
        print("="*60)
        
        confirmation = input("Type 'CONFIRM UPLOAD' to proceed: ")
        if confirmation != "CONFIRM UPLOAD":
            print("❌ Upload cancelled for safety.")
            logger.info("Upload cancelled by user for safety")
            return
        print("✅ Proceeding with upload...")
        logger.info("User confirmed production upload")
    
    # Load CSV
    try:
        df = load_csv_with_encoding(INPUT_CSV)
        logger.info(f"Loaded CSV with {len(df)} rows")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Apply test mode if enabled
        if TEST_MODE:
            original_rows = len(df)
            df = df.head(TEST_ROWS)
            logger.info(f"🧪 TEST_MODE: Processing only first {len(df)} rows out of {original_rows}")
            
    except Exception as e:
        logger.error(f"Failed to load CSV: {e}")
        return
    
    # Determine image URL column
    url_column = None
    for col in ['WOO IMAGE', 's3_url', 'image_url', 'url']:
        if col in df.columns:
            url_column = col
            break
    
    if url_column is None:
        logger.error(f"No recognized image URL column found. Available columns: {list(df.columns)}")
        return
    
    logger.info(f"Using column '{url_column}' for image URLs")
    
    # Determine result column for S3 URLs
    url_result_column = 'NEW IMAGE' if 'NEW IMAGE' in df.columns else 'S3_URL'
    logger.info(f"Will populate '{url_result_column}' column with S3 URLs")
    
    # Initialize result columns
    df['S3_Key'] = ""
    df['Processing_Status'] = ""
    df['HTTP_Response_Code'] = ""
    if url_result_column not in df.columns:
        df[url_result_column] = ""
    
    # Process images
    uploaded_count = 0
    exists_count = 0
    reupload_count = 0
    error_count = 0
    
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing images"):
        url = row[url_column]
        
        # Skip invalid or placeholder URLs
        if pd.isna(url) or str(url).strip().upper() in ['PENDING', 'N/A', 'NA', 'NULL', '']:
            logger.info(f"Row {index + 1}: Skipping invalid/placeholder URL: {url}")
            df.at[index, 'S3_Key'] = ""
            df.at[index, 'Processing_Status'] = "SKIPPED_INVALID_URL"
            df.at[index, 'HTTP_Response_Code'] = 0
            df.at[index, url_result_column] = ""
            error_count += 1
            continue
        
        try:
            s3_key, status, s3_url, response_code = process_single_image(url, index + 1)
            
            df.at[index, 'S3_Key'] = s3_key
            df.at[index, 'Processing_Status'] = status
            df.at[index, 'HTTP_Response_Code'] = response_code
            df.at[index, url_result_column] = s3_url
            
            # Count different types of results
            if status.startswith("UPLOADED") or status.startswith("WOULD_UPLOAD"):
                if status.startswith("WOULD_UPLOAD_EXISTS_403") or status.startswith("UPLOADED") and "403" in status:
                    reupload_count += 1
                else:
                    uploaded_count += 1
            elif status == "EXISTS_OK":
                exists_count += 1
            else:
                error_count += 1
                
        except Exception as e:
            logger.error(f"Unexpected error processing row {index + 1}: {e}")
            df.at[index, 'Processing_Status'] = f"UNEXPECTED_ERROR: {str(e)}"
            df.at[index, 'HTTP_Response_Code'] = 0
            error_count += 1
    
    # Save results
    try:
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"Results saved to: {OUTPUT_CSV}")
    except Exception as e:
        logger.error(f"Failed to save results: {e}")
    
    # Summary
    logger.info("="*60)
    logger.info("PROCESSING SUMMARY")
    logger.info("="*60)
    logger.info(f"Total images processed: {len(df)}")
    logger.info(f"Already existed and accessible: {exists_count}")
    logger.info(f"New uploads: {uploaded_count}")
    logger.info(f"Re-uploads (403/inaccessible): {reupload_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Log file: logs/{log_filename}")
    logger.info("="*60)
    
    print(f"\n✅ Processing completed!")
    print(f"📊 Results: {exists_count} accessible, {uploaded_count} new uploads, {reupload_count} re-uploads, {error_count} errors")
    print(f"📁 Output saved to: {OUTPUT_CSV}")
    print(f"📋 Detailed log: logs/{log_filename}")
    
    # Show status breakdown
    if reupload_count > 0:
        print(f"⚠️  Note: {reupload_count} images were re-uploaded due to accessibility issues (403, etc.)")


if __name__ == "__main__":
    main() 