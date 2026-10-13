import contextlib
import io
import logging
import os
import unittest
from unittest.mock import patch

import pandas as pd
from PIL import Image

from cli import get_config
from csv_handler import CSVHandler
from image_processor import ImageProcessor
from s3_handler import S3Handler


class DeliverooModeTests(unittest.TestCase):
    def config(self, mode, *args):
        with patch.dict(os.environ, {'FOR_DELIVERO': mode}), patch('sys.argv', ['main.py', *args]), contextlib.redirect_stdout(io.StringIO()):
            return get_config()

    def test_deliveroo_pipeline(self):
        config = self.config('true', '--no-backup-originals')
        logger = logging.getLogger('test')
        csv = CSVHandler(config, logger)
        data = pd.DataFrame({'Thumbnail image': ['https://example.com/wp-content/uploads/product.png']})
        self.assertEqual(csv.detect_image_url_column(data), 'Thumbnail image')
        self.assertEqual(csv.determine_result_column(data), 'Thumbnail image Deliveroo')
        dimensions = csv.get_row_target_dimensions(pd.Series({'target_width': 768, 'target_height': 768}))
        self.assertEqual(dimensions, (1200, 800))
        processor = ImageProcessor(config, logger)
        source = io.BytesIO()
        Image.new('RGB', (500, 500), 'red').save(source, 'PNG')
        with patch('image_processor.requests.get') as get:
            get.return_value.content = source.getvalue()
            output = Image.open(processor.process_image_from_url(data.iloc[0, 0], target_width=dimensions[0], target_height=dimensions[1]))
        self.assertEqual(output.size, (1200, 800))
        self.assertEqual(output.format, 'JPEG')
        self.assertEqual(output.getpixel((0, 400)), (255, 255, 255))
        self.assertGreater(output.getpixel((600, 400))[0], 250)
        with patch('s3_handler.boto3.client'):
            s3 = S3Handler(config, logger)
        key = s3.generate_s3_key(data.iloc[0, 0])
        self.assertEqual(key, 'wp-content/uploads/product.jpg')
        self.assertEqual(s3.get_s3_url(key), 'https://deliveroo-bucket-yjh5p6.s3.ap-south-1.amazonaws.com/wp-content/uploads/product.jpg')
        self.assertTrue(config.dry_run)

    def test_conflicting_overrides_rejected(self):
        for args in [('--target-width', '768'), ('--fit-mode', 'cover'), ('--key-prefix', 'wp-content-snoonu'), ('--key-suffix=-cover',), ('--source-column', 'other'), ('--result-column', 'other'), ('--bucket', 'other'), ('--region', 'other')]:
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                self.config('true', *args)
            self.assertEqual(error.exception.code, 2)

    def test_custom_mode_preserves_overrides(self):
        config = self.config('false', '--target-width', '1080', '--target-height', '720', '--key-prefix', 'wp-content-snoonu')
        self.assertFalse(config.for_delivero)
        self.assertEqual((config.target_width, config.target_height), (1080, 720))
        handler = CSVHandler(config, logging.getLogger('test'))
        self.assertEqual(handler.get_row_target_dimensions(pd.Series({'target_width': 640, 'target_height': 480})), (640, 480))

    def test_invalid_flag_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.config('typo')

    def test_custom_filenames_allowed(self):
        config = self.config('TRUE', '--input', 'custom.csv', '--output', 'result.csv')
        self.assertEqual((config.input_csv, config.output_csv), ('custom.csv', 'result.csv'))


if __name__ == '__main__':
    unittest.main()
