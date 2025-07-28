#!/usr/bin/env python3
"""
S3 Handler Module
Handles all S3 operations including existence checks, uploads, and key generation.
"""

import os
import logging
from io import BytesIO
from typing import Tuple
from urllib.parse import urlparse, unquote, quote

import boto3
import requests

from cli import Config


class S3Handler:
    """Handles S3 operations for image storage and retrieval."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize S3 handler with configuration and logger.
        
        Args:
            config: Configuration object with AWS settings
            logger: Logger instance for logging operations
        """
        self.config = config
        self.logger = logger
        
        # Initialize S3 client
        try:
            self.s3 = boto3.client(
                's3',
                region_name=config.aws_region,
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            self.logger.info("S3 client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def extract_s3_path(self, url: str) -> str:
        """Extract S3 path from URL.
        
        Args:
            url: URL to extract path from
            
        Returns:
            S3 path without leading slash
            
        Raises:
            ValueError: If URL is empty or invalid
        """
        if not url or not str(url).strip():
            raise ValueError("Empty or invalid URL")
        
        parsed = urlparse(str(url))
        return parsed.path.lstrip('/')
    
    def generate_s3_key(self, original_url: str) -> str:
        """Generate S3 key from original URL, converting to .jpg format.
        
        Args:
            original_url: Original image URL
            
        Returns:
            Generated S3 key
            
        Raises:
            Exception: If key generation fails
        """
        try:
            # First try the wp-content approach from s3_image_processor.py
            path_start = original_url.find('/wp-content/')
            if path_start != -1:
                key_path = original_url[path_start:]
                base, _ = os.path.splitext(key_path)
                return (base.lstrip('/') + '.jpg')
            
            # Fallback to full path approach
            path = self.extract_s3_path(original_url)
            base, ext = os.path.splitext(path)
            # Convert to .jpg if it's an image extension
            if ext.lower() in ['.png', '.jpeg', '.jpg', '.gif', '.webp']:
                return base + '.jpg'
            return path
            
        except Exception as e:
            self.logger.error(f"Failed to generate S3 key for URL {original_url}: {e}")
            raise
    
    def check_s3_object_exists_and_accessible(self, s3_key: str) -> Tuple[bool, int, str, str]:
        """Check if object exists in S3 bucket and is accessible.
        Tries multiple URL encoding variations to handle encoding mismatches.
        
        Args:
            s3_key: S3 key to check
            
        Returns:
            Tuple of (needs_upload, status_code, status_message, actual_s3_key_found)
        """
        # Generate different encoding variations to try
        s3_key_variations = [
            s3_key,                    # Original key as-is
            unquote(s3_key),          # URL-decoded version  
            quote(s3_key, safe='/')   # URL-encoded version (preserve forward slashes)
        ]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_variations = []
        for key in s3_key_variations:
            if key not in seen:
                seen.add(key)
                unique_variations.append(key)
        
        # Try each variation
        for attempt_key in unique_variations:
            try:
                # First check if object exists in S3
                self.s3.head_object(Bucket=self.config.bucket_name, Key=attempt_key)
                
                # If exists, test actual accessibility by trying to access the URL
                s3_url = f"https://{self.config.bucket_name}.s3.{self.config.aws_region}.amazonaws.com/{quote(attempt_key, safe='/')}"
                response = requests.head(s3_url, timeout=10)
                
                if response.status_code == 200:
                    if attempt_key != s3_key:
                        self.logger.info(f"Found S3 object with different encoding: '{attempt_key}' (instead of '{s3_key}')")
                    return False, 200, "EXISTS_ACCESSIBLE", attempt_key
                elif response.status_code == 403:
                    self.logger.warning(f"S3 object {attempt_key} exists but returns 403 - will re-upload")
                    return True, 403, "EXISTS_403_REUPLOAD", attempt_key
                else:
                    self.logger.warning(f"S3 object {attempt_key} exists but returns {response.status_code} - will re-upload")
                    return True, response.status_code, f"EXISTS_{response.status_code}_REUPLOAD", attempt_key
                    
            except self.s3.exceptions.ClientError as e:
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    # Try next variation
                    continue
                else:
                    self.logger.error(f"Unexpected S3 error for key {attempt_key}: {e}")
                    return True, error_code, f"S3_ERROR_{error_code}", attempt_key
            except requests.RequestException as e:
                self.logger.error(f"Error testing accessibility of {attempt_key}: {e}")
                return True, 0, f"ACCESS_TEST_ERROR", attempt_key
            except Exception as e:
                self.logger.error(f"Error checking S3 object {attempt_key}: {e}")
                return True, 0, f"UNKNOWN_ERROR", attempt_key
        
        # None of the variations were found
        return True, 404, "NOT_EXISTS", s3_key
    
    def upload_to_s3(self, image_buffer: BytesIO, s3_key: str) -> str:
        """Upload image buffer to S3.
        
        Args:
            image_buffer: BytesIO buffer containing image data
            s3_key: S3 key to upload to
            
        Returns:
            Status string ("UPLOADED")
            
        Raises:
            Exception: If upload fails
        """
        try:
            self.s3.upload_fileobj(
                image_buffer,
                self.config.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'image/jpeg', 'ACL': 'public-read'}
            )
            return "UPLOADED"
        except Exception as e:
            self.logger.error(f"Failed to upload to S3 key {s3_key}: {e}")
            raise
    
    def get_s3_url(self, s3_key: str) -> str:
        """Generate public S3 URL for given key.
        
        Args:
            s3_key: S3 key
            
        Returns:
            Public S3 URL
        """
        return f"https://{self.config.bucket_name}.s3.{self.config.aws_region}.amazonaws.com/{quote(s3_key, safe='/')}" 