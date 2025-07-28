# Unified Image Processor

This script combines the functionality of `check_images.py` and `s3_image_processor.py` into a single, robust tool that:

1. **Checks** if images exist in your S3 bucket
2. **Uploads** missing images after processing them (crop/resize)
3. **Logs** all operations with detailed error tracking
4. **Saves** results to CSV for audit trail

## 🚨 SAFETY FIRST

This script handles sensitive data and AWS operations. **Always test before production use.**

## Quick Start

You can use the script in two ways:

1. **CLI Arguments** (recommended - more flexible)
2. **Config file** (config.py holds defaults)

### Method 1: Using CLI Arguments (Recommended)

#### 1. Test Mode (Recommended First Step)

```bash
# Test with first 5 rows, dry run mode, save debug images
python3 main.py --test-mode --test-rows 5 --dry-run --debug-save
```

#### 2. Dry Run (Full Dataset, No Upload)

```bash
# Process all rows, simulate upload only
python3 main.py --dry-run --no-test-mode
```

#### 3. Production Run (Actual Upload)

```bash
# PRODUCTION: Actually upload images to S3
python3 main.py --no-dry-run --no-test-mode
# Script will ask for confirmation: Type 'CONFIRM UPLOAD'
```

#### 4. Custom File and Settings

```bash
# Use different files and settings
python3 main.py \
  --input "my-images.csv" \
  --output "results.csv" \
  --bucket-name "my-bucket" \
  --test-mode --test-rows 10 \
  --dry-run
```

### Method 2: Using Config File Only

```bash
# Edit config.py first, then run:
python3 main.py
```

### Get Help

```bash
# See all available options
python3 main.py --help
```

## Common CLI Usage Examples

```bash
# Test mode: Process first 3 rows only, dry run
python3 main.py --test-mode --test-rows 3

# Dry run with different input file
python3 main.py --input "another-file.csv" --dry-run

# Production upload with different bucket
python3 main.py --no-dry-run --bucket-name "my-new-bucket"

# Debug mode: Save processed images locally
python3 main.py --test-mode --debug-save --debug-dir "my-debug-folder"

# Different image dimensions
python3 main.py --target-width 800 --target-height 600 --dry-run

# Quick test with custom output
python3 main.py --test-mode --test-rows 1 --output "quick-test.csv"
```

## Configuration Options

### CLI Arguments (Recommended)

| CLI Argument      | Short      | Description                     | Default                                     |
| ----------------- | ---------- | ------------------------------- | ------------------------------------------- |
| `--input-csv`     | `--input`  | Input CSV file path             | `'yuhila - missing images.csv'`             |
| `--output-csv`    | `--output` | Output CSV file path            | `'yuhila - missing images - processed.csv'` |
| `--bucket-name`   | `--bucket` | S3 bucket name                  | `'deliveroo-bucket-yjh5p6'`                 |
| `--aws-region`    | `--region` | AWS region                      | `'ap-south-1'`                              |
| `--target-width`  |            | Target image width              | `1200`                                      |
| `--target-height` |            | Target image height             | `800`                                       |
| `--dry-run`       |            | Simulate operations (safe mode) | `True`                                      |
| `--no-dry-run`    | `--upload` | Actually upload to S3           |                                             |
| `--test-mode`     |            | Process only first N rows       | `False`                                     |
| `--no-test-mode`  |            | Process all rows                |                                             |
| `--test-rows`     |            | Number of rows in test mode     | `5`                                         |
| `--debug-save`    |            | Save processed images locally   | `False`                                     |
| `--no-debug-save` |            | Don't save images locally       |                                             |
| `--debug-dir`     |            | Directory for debug images      | `'debug_images'`                            |

### Config File (config.py)

Default values are stored in `config.py` and can be overridden by CLI arguments. Modify `config.py` if you want to change the defaults.

## CSV Input Requirements

Your CSV file must contain one of these columns:

- `WOO IMAGE` (from your current file)
- `s3_url`
- `image_url`
- `url`

## Output

The script creates:

1. **CSV Results**: Original data + 4 columns:

   - `S3_Key`: Generated S3 path
   - `Processing_Status`: Status of the operation (see Status Meanings below)
   - `HTTP_Response_Code`: HTTP response code from accessibility test
   - `NEW IMAGE` (or `S3_URL` if no `NEW IMAGE` column exists): Full public URL to the image

2. **Log File**: Detailed log in `logs/` directory with timestamp

3. **Debug Images** (if `DEBUG_SAVE=True`): Processed images in `debug_images/` directory

## Status Meanings

| Status                             | Meaning                                          |
| ---------------------------------- | ------------------------------------------------ |
| `EXISTS_OK`                        | Image exists in S3 and is accessible (HTTP 200)  |
| `UPLOADED_OK`                      | Image was successfully uploaded and verified     |
| `WOULD_UPLOAD_NOT_EXISTS`          | (Dry run) Would upload new image                 |
| `WOULD_UPLOAD_EXISTS_403_REUPLOAD` | (Dry run) Would re-upload due to 403 error       |
| `UPLOADED_VERIFY_FAIL_XXX`         | Upload completed but verification failed         |
| `SKIPPED_INVALID_URL`              | URL was invalid/placeholder (PENDING, N/A, etc.) |
| `ERROR: reason`                    | Failed to process (see logs for details)         |

### Key Improvements:

- **403 Detection**: Images returning 403 (forbidden) are automatically re-uploaded
- **Accessibility Testing**: Every image is tested for actual accessibility, not just existence
- **Response Codes**: HTTP response codes are shown for each image
- **Smart Skipping**: Invalid URLs like "PENDING" are automatically skipped

## Safety Features

1. **Dry Run Mode**: Simulates all operations without uploading
2. **Test Mode**: Processes only first N rows for testing
3. **Production Confirmation**: Requires typing 'CONFIRM UPLOAD' for actual uploads
4. **Robust Error Handling**: Won't crash on individual image failures
5. **Detailed Logging**: Every operation is logged with timestamps
6. **CSV Encoding Detection**: Automatically handles different file encodings

## Pre-Upload Checklist

- [ ] CSV file exists and has the right column name
- [ ] AWS credentials are set in environment variables
- [ ] S3 bucket name and region are correct in config.py
- [ ] Test mode completed successfully
- [ ] Dry run completed successfully
- [ ] Reviewed the generated CSV and logs
- [ ] Ready for production run

## Environment Variables Required

```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

## Dependencies

Install via: `pip install -r requirements.txt`

- boto3
- pandas
- Pillow
- python-dotenv
- requests
- tqdm

## Troubleshooting

**CSV encoding errors**: The script automatically tries multiple encodings (utf-8, latin-1, cp1252, iso-8859-1)

**AWS errors**: Check your credentials and S3 bucket permissions

**Image processing errors**: Check that URLs are accessible and point to valid images

**Memory issues**: For large datasets, the script processes images one at a time to minimize memory usage

## File Structure After Running

```
├── main.py      # Main script
├── config.py                       # Configuration
├── yuhila - missing images.csv     # Input data
├── yuhila - missing images - processed.csv  # Results
├── logs/
│   └── unified_processor_YYYYMMDD_HHMMSS.log
└── debug_images/                   # (if DEBUG_SAVE=True)
    └── processed_image_files.jpg
```
