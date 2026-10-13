# Image processor and S3 uploader

Download product images from a CSV, resize them, upload JPEGs to S3, and write image URLs and processing statuses to a separate CSV.

## Setup

Run commands from this repository. The existing virtual environment can be used directly:

```bash
env/bin/python main.py --help
```

For a fresh installation, create a virtual environment and install `requirements.txt` with pip. Dependencies include Pillow, requests, pandas, boto3, python-dotenv, and tqdm.

Create `.env` using `.env.example` if it does not already exist, then fill in your AWS credentials. Never commit `.env`.

```dotenv
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-south-1
FOR_DELIVERO=true
```

AWS credentials need access to the destination bucket for object checks and uploads. Uploads use the `public-read` ACL, so the bucket must allow it. Shell environment variables take precedence over `.env` values.

## Deliveroo mode

Set `FOR_DELIVERO=true`. The spelling is intentional. Only rows with a blank (including whitespace-only) `Thumbnail image Deliveroo` are processed. Filled rows are preserved without downloading or checking their images. The full sheet is saved to the output CSV, with verified upload links filled into that same column; the input file remains unchanged. Product identifiers retain leading zeros.

This mode selects:

| Setting | Value |
| --- | --- |
| Input CSV | `Delivero list - with thumbnails.csv` |
| Output CSV | `deliveroo-processed.csv` |
| Source column | `Thumbnail image` |
| Result column | `Thumbnail image Deliveroo` |
| Image | 1200 × 800, 3:2, JPEG quality 85 |
| Fit | `contain`: full image centered on white padding |
| Bucket | `deliveroo-bucket-yjh5p6` |
| Region | `ap-south-1` |
| Object path | Original `wp-content/uploads/...` hierarchy, JPEG extension, no extra prefix or suffix |

Example: `https://m.yuehlia.com/wp-content/uploads/2021/01/product.png` becomes `https://deliveroo-bucket-yjh5p6.s3.ap-south-1.amazonaws.com/wp-content/uploads/2021/01/product.jpg`.

Deliveroo mode ignores CSV `target_width` / `target_height` values and rejects conflicting CLI overrides for its image, bucket, path, and column settings. Input/output filenames and processing controls remain configurable.

The [Deliveroo retail image guidelines](https://help.deliveroo.com/en/articles/9702025-how-to-upload-your-own-product-images-retail-partners) specify 3:2 item images with a minimum size of 1200 × 800. The platform can crop these to square for display; keep the product centered. Enlarging a small source does not add detail.

### Preview one product

```bash
env/bin/python main.py --dry-run --test-mode --test-rows 1 \
  --debug-save --output deliveroo-preview.csv
```

Test mode selects the first missing Deliveroo URL, retaining every row in the output. Inspect `debug_images/` and `deliveroo-preview.csv`. Dry runs still check S3 and write the output CSV. Images are downloaded and processed when debug saving or original backups are enabled. In Deliveroo mode, dry runs leave missing URL cells blank. Failed uploads or failed verification also leave them blank for retry.

### Upload all products

```bash
env/bin/python main.py --no-dry-run --no-test-mode
```

Enter `CONFIRM UPLOAD` when prompted. Existing matching objects are overwritten by default; use `--skip-existing` to retain accessible objects. Test mode limits rows but does not prevent uploads when `--no-dry-run` is used.

### Use another Deliveroo CSV

The same thumbnail headers are required:

```bash
env/bin/python main.py --input another-deliveroo.csv \
  --output another-deliveroo-processed.csv --dry-run --debug-save
```

## Custom / Snoono mode

Set `FOR_DELIVERO=false` in `.env`, or override it for one command. Missing `FOR_DELIVERO` defaults to false; values other than true/false are rejected (case-insensitive).

In custom mode, CLI options override `config.py`. Current defaults are 768 × 768, `contain`, the Deliveroo bucket, and `export.csv` → `export-processed.csv`. Setting the flag false does not switch buckets automatically. Bucket/region come from `config.py` or CLI; `AWS_REGION` in `.env` does not override them.

```bash
FOR_DELIVERO=false env/bin/python main.py \
  --input snoono.csv --output snoono-preview.csv \
  --source-column 'Thumbnail 1000x1000' \
  --result-column 'Cover 1080x720 3:2' \
  --target-width 1080 --target-height 720 --fit-mode contain \
  --key-prefix wp-content-snoonu --key-suffix=-cover-1080x720 \
  --dry-run --test-mode --test-rows 1 --debug-save
```

For the full upload, replace `--dry-run --test-mode --test-rows 1` with `--no-dry-run --no-test-mode` and select a separate output filename.

| Option | Purpose |
| --- | --- |
| `--input`, `--output` | Input and output CSV paths |
| `--bucket-name`, `--aws-region` | Destination bucket and region |
| `--source-column`, `--result-column` | Source URL and output URL columns |
| `--target-width`, `--target-height` | Output pixels; row dimensions override in custom mode |
| `--fit-mode contain` | Preserve whole image with white padding |
| `--fit-mode cover` | Fill canvas and center-crop overflow |
| `--key-prefix` | Replace `wp-content` root, or prepend to other paths |
| `--key-suffix` | Append suffix before image extension |
| `--skip-existing` | Skip accessible destination objects |
| `--overwrite-existing` | Reprocess existing objects (default) |
| `--debug-save`, `--debug-dir` | Save processed previews; directory defaults to `debug_images` |
| `--no-backup-originals` | Disable backups (enabled by default) |
| `--backup-dir` | Backup directory; default `original_image_backups` |
| `--test-mode`, `--test-rows` | Limit processing to first N rows (default 5) |

Without explicit custom column settings, source detection checks `thumbnail_url_bucket`, `Thumbnail 1000x1000`, `WOO IMAGE`, `s3_url`, `image_url`, then `url`. Results use `NEW IMAGE` if present, otherwise `S3_URL`.

## Results and troubleshooting

The output CSV contains the result URL plus `S3_Key`, `Processing_Status`, `HTTP_Response_Code`, `Applied_Target_Width`, and `Applied_Target_Height`. Review statuses for errors and upload verification failures. Logs are saved under `logs/`; original downloads under `original_image_backups/`.

- Missing column: verify headers or choose explicit columns in custom mode.
- AWS access error: check credentials, bucket permissions, and public-read ACL support.
- Wrong dimensions/path: verify `FOR_DELIVERO` and shell overrides; custom mode also honors row dimensions.
- Download error: verify the original image URL is accessible.

Run offline regression checks with:

```bash
env/bin/python -m unittest discover -s tests -v
```
