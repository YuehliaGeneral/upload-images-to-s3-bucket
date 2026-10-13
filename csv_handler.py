#!/usr/bin/env python3
"""
CSV Handler Module
Handles CSV loading, encoding detection, column mapping, and result saving.
"""

import logging
from typing import Optional, Tuple

import pandas as pd

from cli import Config


class CSVHandler:
    """Handles CSV file operations with robust encoding and column detection."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize CSV handler with configuration and logger.
        
        Args:
            config: Configuration object with file paths
            logger: Logger instance for logging operations
        """
        self.config = config
        self.logger = logger
    
    def load_csv_with_encoding(self, file_path: str) -> pd.DataFrame:
        """Load CSV with robust encoding handling.
        
        Args:
            file_path: Path to CSV file to load
            
        Returns:
            Loaded pandas DataFrame
            
        Raises:
            ValueError: If CSV cannot be read with any encoding
        """
        encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings_to_try:
            try:
                options = {'dtype': str, 'keep_default_na': False} if self.config.for_delivero else {}
                df = pd.read_csv(file_path, encoding=encoding, **options)
                self.logger.info(f"Successfully loaded CSV using {encoding} encoding")
                return df
            except UnicodeDecodeError:
                continue
        
        raise ValueError(f"Could not read {file_path} with any of the tried encodings: {encodings_to_try}")
    
    def detect_image_url_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect the column containing image URLs.
        
        Args:
            df: DataFrame to search for image URL column
            
        Returns:
            Column name if found, None otherwise
        """
        if self.config.source_column:
            if self.config.source_column in df.columns:
                self.logger.info(f"Using configured source column '{self.config.source_column}' for image URLs")
                return self.config.source_column

            self.logger.error(
                f"Configured source column '{self.config.source_column}' was not found. "
                f"Available columns: {list(df.columns)}"
            )
            return None

        possible_columns = ['thumbnail_url_bucket', 'Thumbnail 1000x1000', 'WOO IMAGE', 's3_url', 'image_url', 'url']
        
        for col in possible_columns:
            if col in df.columns:
                self.logger.info(f"Using column '{col}' for image URLs")
                return col
        
        self.logger.error(f"No recognized image URL column found. Available columns: {list(df.columns)}")
        return None
    
    def determine_result_column(self, df: pd.DataFrame) -> str:
        """Determine the column name for storing S3 URLs.
        
        Args:
            df: DataFrame to check for existing result column
            
        Returns:
            Column name to use for results
        """
        if self.config.result_column:
            result_column = self.config.result_column
        else:
            result_column = 'NEW IMAGE' if 'NEW IMAGE' in df.columns else 'S3_URL'

        self.logger.info(f"Will populate '{result_column}' column with S3 URLs")
        return result_column
    
    def initialize_result_columns(self, df: pd.DataFrame, result_column: str) -> pd.DataFrame:
        """Initialize result columns in the DataFrame.
        
        Args:
            df: DataFrame to initialize columns in
            result_column: Name of the column for S3 URLs
            
        Returns:
            DataFrame with initialized result columns
        """
        for column in ('S3_Key', 'Processing_Status', 'HTTP_Response_Code',
                       'Applied_Target_Width', 'Applied_Target_Height'):
            if not self.config.for_delivero or column not in df.columns:
                df[column] = ""
        
        if result_column not in df.columns:
            df[result_column] = ""
        
        return df

    def get_row_target_dimensions(self, row: pd.Series) -> Tuple[int, int]:
        """Return row-specific target dimensions, falling back to config defaults."""
        if self.config.for_delivero:
            return self.config.target_width, self.config.target_height
        target_width = self.config.target_width
        target_height = self.config.target_height

        if 'target_width' in row and pd.notna(row['target_width']):
            target_width = int(row['target_width'])
        if 'target_height' in row and pd.notna(row['target_height']):
            target_height = int(row['target_height'])

        if target_width <= 0 or target_height <= 0:
            raise ValueError(f"Invalid target dimensions: {target_width}x{target_height}")

        return target_width, target_height
    
    def load_data(self) -> Tuple[pd.DataFrame, str, str]:
        """Load CSV data and determine column mappings.
        
        Returns:
            Tuple of (dataframe, url_column_name, result_column_name)
            
        Raises:
            Exception: If CSV loading or column detection fails
        """
        # Load CSV
        df = self.load_csv_with_encoding(self.config.input_csv)
        self.logger.info(f"Loaded CSV with {len(df)} rows")
        self.logger.info(f"Columns: {list(df.columns)}")
        
        # Apply test mode if enabled
        if self.config.test_mode and not self.config.for_delivero:
            original_rows = len(df)
            df = df.head(self.config.test_rows)
            self.logger.info(f"🧪 TEST_MODE: Processing only first {len(df)} rows out of {original_rows}")
        
        # Detect columns
        url_column = self.detect_image_url_column(df)
        if url_column is None:
            raise ValueError("No recognized image URL column found")
        
        result_column = self.determine_result_column(df)
        
        # Initialize result columns
        df = self.initialize_result_columns(df, result_column)
        
        return df, url_column, result_column

    def rows_to_process(self, df: pd.DataFrame, result_column: str) -> pd.DataFrame:
        """Select missing Deliveroo URLs while keeping the full output sheet."""
        if not self.config.for_delivero:
            return df
        values = df[result_column]
        pending = df[values.isna() | values.astype(str).str.strip().eq('')]
        self.logger.info(f'Deliveroo: {len(pending)} missing URLs; {len(df) - len(pending)} filled rows preserved')
        if self.config.test_mode:
            pending = pending.head(self.config.test_rows)
        return pending
    
    def save_results(self, df: pd.DataFrame) -> None:
        """Save processed results to CSV file.
        
        Args:
            df: DataFrame with results to save
            
        Raises:
            Exception: If saving fails
        """
        try:
            df.to_csv(self.config.output_csv, index=False, encoding='utf-8')
            self.logger.info(f"Results saved to: {self.config.output_csv}")
        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")
            raise 
