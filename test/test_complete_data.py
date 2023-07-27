import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from datetime import datetime
from src.complete_data import CompleteData

class TestCompleteData(unittest.TestCase):

    def setUp(self):
        # Create a temporary test directory to store downloaded files
        self.root_data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','data'))
        self.start_date = datetime(2023, 6, 1)
        self.country = 'ETHIOPIA'
        self.cores = 2
        self.test_path = './test_files'
        os.makedirs(self.test_path, exist_ok=True)

    def tearDown(self):
        # Clean up the temporary test directory and its contents after each test
        for filename in os.listdir(self.test_path):
            file_path = os.path.join(self.test_path, filename)
            os.remove(file_path)
        os.rmdir(self.test_path)

    def test_download_file_force_true(self):
        # Test downloading a file with force=True
        file_name = f"chirp.{self.start_date.strftime('%Y.%m.%d')}.tif.gz"
        url = f"http://data.chc.ucsb.edu/products/CHIRP/daily/{str(self.start_date.year)}/{file_name}"
        file_path = os.path.join(self.test_path, file_name)
        complete_data = CompleteData(self.start_date, self.country, self.test_path)
        
        # Ensure the file does not exist before downloading
        self.assertFalse(os.path.exists(file_path))
        
        # Perform the download with force=True
        complete_data.download_file(url, file_path, force=True)
        
        # Check if the file was downloaded and extracted
        self.assertTrue(os.path.exists(file_path.replace('.gz', '')))

    def test_download_file_force_false(self):
        # Test downloading a file with force=False (file already exists)
        file_name = f"chirp.{self.start_date.strftime('%Y.%m.%d')}.tif.gz"
        url = f"http://data.chc.ucsb.edu/products/CHIRP/daily/{str(self.start_date.year)}/{file_name}"
        file_path = os.path.join(self.test_path, file_name)
        complete_data = CompleteData(self.start_date, self.country, self.test_path)
        
        # Create an empty file to simulate an already downloaded file
        open(file_path, 'w').close()
        
        # Perform the download with force=False
        complete_data.download_file(url, file_path, force=False)
        
        # Ensure the file was not downloaded again
        self.assertFalse(os.path.exists(file_path.replace('.gz', '')))

    def test_download_file_path_not_exists(self):
        # Test downloading a file to a path that does not exist
        file_name = f"chirp.{self.start_date.strftime('%Y.%m.%d')}.tif.gz"
        url = f"http://data.chc.ucsb.edu/products/CHIRP/daily/{str(self.start_date.year)}/{file_name}"
        file_path = os.path.join(self.test_path, self.country, file_name)
        complete_data = CompleteData(self.start_date, self.country, self.test_path)
        
        # Ensure the subdirectory does not exist before downloading
        self.assertFalse(os.path.exists(os.path.dirname(file_path)))
        
        # Perform the download with force=True to create the subdirectory and download the file
        complete_data.download_file(url, file_path, force=True)
        
        # Check if the file was downloaded and extracted
        self.assertTrue(os.path.exists(file_path.replace('.gz', '')))

if __name__ == '__main__':
    unittest.main()