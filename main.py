#!/usr/bin/env python3
"""
Unified Image Processor
Checks for missing images in S3 bucket and uploads them if they don't exist.
Clean orchestrator using modular components.
"""

import os
import pandas as pd
import logging
from datetime import datetime
from tqdm import tqdm
from typing import Tuple

from dotenv import load_dotenv
load_dotenv()

# Import our modular components
from cli import get_config
from image_processor import ImageProcessor
from s3_handler import S3Handler
from csv_handler import CSVHandler


def setup_logging(config) -> logging.Logger:
    """Set up logging configuration."""
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
    return logger


def process_single_image(url: str, row_index: int, image_processor: ImageProcessor, 
                        s3_handler: S3Handler, config, logger: logging.Logger) -> Tuple[str, str, str, int]:
    """Process a single image: check existence/accessibility and upload if needed.
    
    Args:
        url: Image URL to process
        row_index: Row index for logging
        image_processor: ImageProcessor instance
        s3_handler: S3Handler instance
        config: Configuration object
        logger: Logger instance
    
    Returns:
        Tuple of (s3_key, status, s3_url, response_code)
    """
    try:
        # Generate S3 key
        s3_key = s3_handler.generate_s3_key(url)
        s3_url = s3_handler.get_s3_url(s3_key)
        
        # Check if exists and is accessible
        needs_upload, status_code, check_status, actual_s3_key = s3_handler.check_s3_object_exists_and_accessible(s3_key)
        
        if not needs_upload:
            # Use the actual key that was found (might have different encoding)
            actual_s3_url = s3_handler.get_s3_url(actual_s3_key)
            logger.info(f"Row {row_index}: Image exists and accessible (HTTP {status_code}) - {actual_s3_key}")
            return actual_s3_key, "EXISTS_OK", actual_s3_url, status_code
        
        # Use the actual key that was found for upload (handles encoding issues)
        upload_s3_key = actual_s3_key
        upload_s3_url = s3_handler.get_s3_url(upload_s3_key)
        
        # Log why we need to upload
        if check_status.startswith("EXISTS_403"):
            logger.info(f"Row {row_index}: Image exists but 403 forbidden (HTTP {status_code}) - will re-upload - {upload_s3_key}")
        elif check_status.startswith("EXISTS_"):
            logger.info(f"Row {row_index}: Image exists but HTTP {status_code} - will re-upload - {upload_s3_key}")
        elif check_status == "NOT_EXISTS":
            logger.info(f"Row {row_index}: Image not found (HTTP 404) - will upload - {upload_s3_key}")
        else:
            logger.info(f"Row {row_index}: {check_status} - will upload - {upload_s3_key}")
        
        if config.dry_run:
            logger.info(f"Row {row_index}: [DRY RUN] Would upload - {upload_s3_key}")
            return upload_s3_key, f"WOULD_UPLOAD_{check_status}", upload_s3_url, status_code
        
        # Process and upload image
        debug_filename = os.path.basename(upload_s3_key) if config.debug_save else None
        image_buffer = image_processor.process_image_from_url(url, debug_filename)
        upload_result = s3_handler.upload_to_s3(image_buffer, upload_s3_key)
        
        # Verify upload worked by checking again
        verify_needs_upload, verify_status_code, verify_status, verify_actual_key = s3_handler.check_s3_object_exists_and_accessible(upload_s3_key)
        if not verify_needs_upload and verify_status_code == 200:
            logger.info(f"Row {row_index}: Successfully uploaded and verified (HTTP {verify_status_code}) - {upload_s3_key}")
            return upload_s3_key, "UPLOADED_OK", upload_s3_url, verify_status_code
        else:
            logger.warning(f"Row {row_index}: Upload completed but verification failed (HTTP {verify_status_code}) - {upload_s3_key}")
            return upload_s3_key, f"UPLOADED_VERIFY_FAIL_{verify_status_code}", upload_s3_url, verify_status_code
        
    except Exception as e:
        logger.error(f"Row {row_index}: Failed to process {url} - {e}")
        return "", f"ERROR: {str(e)}", "", 0


def confirm_production_run(config, logger: logging.Logger) -> bool:
    """Confirm production run with user if needed."""
    if not config.dry_run and not config.test_mode:
        print("\n" + "="*60)
        print("⚠️  WARNING: PRODUCTION MODE ENABLED")
        print("="*60)
        print("This will ACTUALLY UPLOAD images to S3!")
        print(f"Bucket: {config.bucket_name}")
        print(f"CSV file: {config.input_csv}")
        print("This action cannot be undone.")
        print("="*60)
        
        confirmation = input("Type 'CONFIRM UPLOAD' to proceed: ")
        if confirmation != "CONFIRM UPLOAD":
            print("❌ Upload cancelled for safety.")
            logger.info("Upload cancelled by user for safety")
            return False
        print("✅ Proceeding with upload...")
        logger.info("User confirmed production upload")
    
    return True


def main():
    """Main processing function."""
    # Get configuration
    config = get_config()
    
    # Set up logging
    logger = setup_logging(config)
    
    logger.info("="*60)
    logger.info("UNIFIED IMAGE PROCESSOR STARTED")
    logger.info("="*60)
    logger.info(f"Input CSV: {config.input_csv}")
    logger.info(f"Output CSV: {config.output_csv}")
    logger.info(f"S3 Bucket: {config.bucket_name}")
    logger.info(f"Target Dimensions: {config.target_width}x{config.target_height}")
    logger.info(f"Dry Run Mode: {config.dry_run}")
    logger.info(f"Debug Save: {config.debug_save}")
    logger.info(f"Test Mode: {config.test_mode}")
    if config.test_mode:
        logger.info(f"Test Rows: {config.test_rows}")
    
    # Safety confirmation for production runs
    if not confirm_production_run(config, logger):
        return
    
    # Initialize handlers
    try:
        csv_handler = CSVHandler(config, logger)
        image_processor = ImageProcessor(config, logger)
        s3_handler = S3Handler(config, logger)
    except Exception as e:
        logger.error(f"Failed to initialize handlers: {e}")
        return
    
    # Load data
    try:
        df, url_column, result_column = csv_handler.load_data()
    except Exception as e:
        logger.error(f"Failed to load CSV data: {e}")
        return
    
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
            df.at[index, result_column] = ""
            error_count += 1
            continue
        
        try:
            s3_key, status, s3_url, response_code = process_single_image(
                url, index + 1, image_processor, s3_handler, config, logger
            )
            
            df.at[index, 'S3_Key'] = s3_key
            df.at[index, 'Processing_Status'] = status
            df.at[index, 'HTTP_Response_Code'] = response_code
            df.at[index, result_column] = s3_url
            
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
        csv_handler.save_results(df)
    except Exception as e:
        logger.error(f"Failed to save results: {e}")
        return
    
    # Summary
    logger.info("="*60)
    logger.info("PROCESSING SUMMARY")
    logger.info("="*60)
    logger.info(f"Total images processed: {len(df)}")
    logger.info(f"Already existed and accessible: {exists_count}")
    logger.info(f"New uploads: {uploaded_count}")
    logger.info(f"Re-uploads (403/inaccessible): {reupload_count}")
    logger.info(f"Errors: {error_count}")
    logger.info("="*60)
    
    print(f"\n✅ Processing completed!")
    print(f"📊 Results: {exists_count} accessible, {uploaded_count} new uploads, {reupload_count} re-uploads, {error_count} errors")
    print(f"📁 Output saved to: {config.output_csv}")
    
    # Show status breakdown
    if reupload_count > 0:
        print(f"⚠️  Note: {reupload_count} images were re-uploaded due to accessibility issues (403, etc.)")


if __name__ == "__main__":
    main() 