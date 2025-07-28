# Default configuration for unified_image_processor.py
# These values can be overridden by command line arguments
# Use --help to see available CLI options

# Input/Output files
INPUT_CSV = 'yuhila - missing images.csv'
OUTPUT_CSV = 'yuhila - missing images - processed.csv'

# AWS S3 Settings
BUCKET_NAME = 'deliveroo-bucket-yjh5p6'
AWS_REGION = 'ap-south-1'

# Image Processing Settings
TARGET_WIDTH = 1200
TARGET_HEIGHT = 800

# Safety Settings (DEFAULTS - can override with CLI)
DRY_RUN = True  # Default to safe mode - use --no-dry-run to upload
DEBUG_SAVE = False  # Set to True to save processed images locally
DEBUG_DIR = 'debug_images'

# Test Settings (for limited testing)
TEST_MODE = False  # Default to process all rows - use --test-mode for testing
TEST_ROWS = 5  # Number of rows to process in test mode

# CLI Examples:
# python3 unified_image_processor.py --help
# python3 unified_image_processor.py --test-mode --test-rows 10
# python3 unified_image_processor.py --no-dry-run --input "my-file.csv" 
# python3 unified_image_processor.py --debug-save --bucket-name "my-bucket" 