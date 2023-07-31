import sys
import os
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from datetime import datetime
from src.complete_data import CompleteData

class TestCompleteData(unittest.TestCase):

    def setUp(self):
        # Create a temporary test directory to store downloaded files
        self.start_date = datetime(2023, 6, 1)
        self.cores = 2
        self.root_data = os.path.abspath(os.path.join(os.path.dirname(__file__), 'test_files'))
        self.country = 'ETHIOPIA'
        self.chirps_url_name = f"chirp.{self.start_date.strftime('%Y.%m.%d')}.tif.gz"
        self.chirps_file_name = f"chirp.{self.start_date.strftime('%Y.%m.%d')}.tif"
        self.inputs_prediccion = os.path.join(self.root_data,self.country,"inputs","prediccionClimatica")
        self.daily_downloaded_path = os.path.join(self.inputs_prediccion, "daily_downloaded")
        self.chirps_path = os.path.join(self.daily_downloaded_path, "chirps")
        self.chirps_file_path = os.path.join(self.chirps_path, self.chirps_file_name)
        self.chirps_file_path_compressed = os.path.join(self.chirps_path, self.chirps_url_name)
        self.url = f"http://data.chc.ucsb.edu/products/CHIRP/daily/{str(self.start_date.year)}/{self.chirps_url_name}"
        os.makedirs(self.chirps_path, exist_ok=True)

    def tearDown(self):
        # Clean up the temporary test directory and its contents after each test
        #shutil.rmtree(self.root_data)
        pass

    def test_download_file_force_false(self):
        # Test downloading a file with force=False (file already exists)
        complete_data = CompleteData(self.start_date, self.country, self.root_data, cores=self.cores)

        # Create an empty file to simulate an already downloaded file
        if not(os.path.exists(self.chirps_file_path)):
            os.makedirs(self.chirps_path, exist_ok=True)
            open(self.chirps_file_path, 'w').close()

        # Perform the download with force=False
        complete_data.download_file(self.url, self.chirps_file_path_compressed, force=False)

        # Ensure the file was not downloaded again
        self.assertTrue(os.path.exists(self.chirps_file_path))

    def test_download_file_force_true(self):
        # Test downloading a file with force=True
        complete_data = CompleteData(self.start_date, self.country, self.root_data, cores=self.cores)
        
        # Ensure the file exist before downloading
        self.assertTrue(os.path.exists(self.chirps_file_path))
        
        # Perform the download with force=True
        complete_data.download_file(self.url, self.chirps_file_path_compressed, force=True)
        
        # Check if the file was downloaded and extracted
        self.assertTrue(os.path.exists(self.chirps_file_path))

    def test_download_file_path_not_exists(self):
        # Test downloading a file to a path that does not exist
        complete_data = CompleteData(self.start_date, self.country, self.root_data, cores=self.cores)
        
        # Ensure the subdirectory does not exist before downloading
        if os.path.exists(self.chirps_file_path):
            os.remove(self.chirps_file_path)
        self.assertFalse(os.path.exists(self.chirps_file_path))
        
        # Perform the download with force=True to create the subdirectory and download the file
        complete_data.download_file(self.url, self.chirps_file_path_compressed, force=True)
        
        # Check if the file was downloaded and extracted
        self.assertTrue(os.path.exists(self.chirps_file_path))

if __name__ == '__main__':
    unittest.main()