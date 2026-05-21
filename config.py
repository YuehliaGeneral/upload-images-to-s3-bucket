# Default configuration for main.py
# These values can be overridden by command line arguments
# Use --help to see available CLI options

# Input/Output files
INPUT_CSV = 'export.csv'
OUTPUT_CSV = 'export-processed.csv'

# AWS S3 Settings
BUCKET_NAME = 'bucket-p67ztf'
AWS_REGION = 'ap-south-1'

# Image Processing Settings
# Rows with target_width/target_height columns override these defaults.
TARGET_WIDTH = 768
TARGET_HEIGHT = 768

# Safety Settings (DEFAULTS - can override with CLI)
DRY_RUN = True  # Default to safe mode - use --no-dry-run to upload
OVERWRITE_EXISTING = True  # Reprocess and upload even when the S3 object already exists
BACKUP_ORIGINALS = True  # Save downloaded original images locally before processing
BACKUP_DIR = 'original_image_backups'
DEBUG_SAVE = False  # Set to True to save processed images locally
DEBUG_DIR = 'debug_images'

# Test Settings (for limited testing)
TEST_MODE = False  # Default to process all rows - use --test-mode for testing
TEST_ROWS = 5  # Number of rows to process in test mode

# CLI Examples:
# python3 main.py --help
# python3 main.py --test-mode --test-rows 10
# python3 main.py --no-dry-run --input "my-file.csv" 
# python3 main.py --debug-save --bucket-name "my-bucket" 
