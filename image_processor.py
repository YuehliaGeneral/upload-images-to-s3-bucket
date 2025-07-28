#!/usr/bin/env python3
"""
Image Processor Module
Handles image downloading, processing, cropping, and resizing operations.
"""

import os
import logging
from io import BytesIO
from typing import Optional

import requests
from PIL import Image

from cli import Config


class ImageProcessor:
    """Handles image processing operations including download, crop, and resize."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize image processor with configuration and logger.
        
        Args:
            config: Configuration object with processing settings
            logger: Logger instance for logging operations
        """
        self.config = config
        self.logger = logger
        
        # Create debug directory if needed
        if self.config.debug_save:
            os.makedirs(self.config.debug_dir, exist_ok=True)
    
    def center_crop_image(self, img: Image.Image) -> Image.Image:
        """Resize image to fit within target dimensions with white background, no crop.
        
        Args:
            img: PIL Image to process
            
        Returns:
            Processed PIL Image
        """
        img_ratio = img.width / img.height
        target_ratio = self.config.target_width / self.config.target_height

        if img_ratio > target_ratio:
            new_width = self.config.target_width
            new_height = int(self.config.target_width / img_ratio)
        else:
            new_height = self.config.target_height
            new_width = int(self.config.target_height * img_ratio)

        resized_img = img.resize((new_width, new_height), Image.LANCZOS)
        new_img = Image.new("RGB", (self.config.target_width, self.config.target_height), (255, 255, 255))
        paste_x = (self.config.target_width - new_width) // 2
        paste_y = (self.config.target_height - new_height) // 2
        new_img.paste(resized_img, (paste_x, paste_y))
        return new_img
    
    def process_image_from_url(self, url: str, debug_filename: Optional[str] = None) -> BytesIO:
        """Download and process image from URL.
        
        Args:
            url: URL of the image to download and process
            debug_filename: Optional filename for saving debug image
            
        Returns:
            BytesIO buffer containing processed JPEG image
            
        Raises:
            Exception: If image download or processing fails
        """
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            img = Image.open(BytesIO(response.content)).convert('RGB')
            img = self.center_crop_image(img)

            if self.config.debug_save and debug_filename:
                debug_path = os.path.join(self.config.debug_dir, debug_filename)
                img.save(debug_path, format='JPEG', quality=85)
                self.logger.debug(f"Debug image saved: {debug_path}")

            buffer = BytesIO()
            img.save(buffer, format='JPEG', quality=85)
            buffer.seek(0)
            return buffer
            
        except Exception as e:
            self.logger.error(f"Failed to process image from URL {url}: {e}")
            raise 