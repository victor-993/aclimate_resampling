import sys
import os
import shutil
import glob

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from datetime import datetime
from datetime import timedelta
from src.complete_data import CompleteData
import pandas as pd

class TestCompleteData(unittest.TestCase):

    def setUp(self):
        # Create a temporary test directory to store downloaded files
        self.start_date = datetime(2023, 6, 1)
        self.end_date = datetime(2023, 6, 3)  # Only 3 days for this test
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
        self.era5_path = os.path.join(self.daily_downloaded_path, "era5")
        self.data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        self.variable_era5 = "t_max"
        self.chirp_data = "chirp.2023.06.01.tif"
        self.era5_data = "Temperature-Air-2m-Max-24h_C3S-glob-agric_AgERA5_20230601_final-v1.0.tif"
        self.location = pd.DataFrame({'ws': ['Test Location'], 'lat': [6.4095], 'lon': [-72.0211]})
        self.locations = pd.DataFrame({ 'ws': ['Location 1', 'Location 2'], 'lat': [6.4095, 6.3830], 'lon': [-72.0211, -71.8700]})
        os.makedirs(self.chirps_path, exist_ok=True)
        os.makedirs(os.path.join(self.era5_path,self.variable_era5), exist_ok=True)

        # Leap year
        self.start_date_leapyear = datetime(2020, 2, 1)

    def tearDown(self):
        # Clean up the temporary test directory and its contents after each test
        shutil.rmtree(self.root_data)
        pass

    def create_mock_raster(self):
        chirp_src = os.path.join(self.data,self.chirp_data)
        era5_src = os.path.join(self.data,self.era5_data)
        chirp_dst = os.path.join(self.chirps_path,self.chirp_data)
        era5_dst = os.path.join(self.era5_path,self.variable_era5,self.era5_data)
        if not(os.path.exists(chirp_dst)):
            shutil.copyfile(chirp_src, chirp_dst)
        if not(os.path.exists(era5_src)):
            shutil.copyfile(era5_src, era5_dst)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # TEST DOWNLOAD FILE
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

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
        
        # Create an empty file to simulate an already downloaded file
        if not(os.path.exists(self.chirps_file_path)):
            os.makedirs(self.chirps_path, exist_ok=True)
            open(self.chirps_file_path, 'w').close()
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

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # TEST DOWNLOAD DATA CHIRP
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def test_download_data_chirp(self):
        # Test downloading chirp data for a specific period
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Perform the download
        complete_data.download_data_chirp(self.daily_downloaded_path, year_to=self.start_date.year)

        # Check if the chirp data files were downloaded and stored in the correct location
        dates = [self.start_date + timedelta(days=x) for x in range((self.end_date - self.start_date).days + 1)]
        expected_files = [f"chirp.{date.strftime('%Y.%m.%d')}.tif" for date in dates]
        for file in expected_files:
            file_path = os.path.join(self.chirps_path, file)
            self.assertTrue(os.path.exists(file_path))

    def test_download_data_chirp_existing_files(self):
        # Test downloading chirp data when some files already exist and force=False
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Create some mock chirp data files
        for date in [self.start_date + timedelta(days=1), self.start_date + timedelta(days=2)]:
            file_name = f"chirp.{date.strftime('%Y.%m.%d')}.tif"
            file_path = os.path.join(self.chirps_path, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            if os.path.exists(file_path):
                os.remove(file_path)
            with open(file_path, 'w') as f:
                f.write("Mock data")

        # Perform the download
        complete_data.download_data_chirp(self.daily_downloaded_path, year_to=self.start_date.year)

        # Check if the existing files were not downloaded again
        for date in [self.start_date + timedelta(days=1), self.start_date + timedelta(days=2)]:
            file_name = f"chirp.{date.strftime('%Y.%m.%d')}.tif"
            file_path = os.path.join(self.chirps_path, file_name)
            self.assertTrue(os.path.exists(file_path))
            self.assertEqual(os.path.getsize(file_path), 9)  # Size of the "Mock data"

    def test_download_data_chirp_leapyear(self):
        # Test downloading chirp data for a leap year
        complete_data = CompleteData(start_date=self.start_date_leapyear, country=self.country, path=self.root_data, cores=self.cores)

        # Perform the download
        complete_data.download_data_chirp(self.daily_downloaded_path, year_to=self.start_date_leapyear.year)

        # Check if the chirp data files were downloaded and stored in the correct location
        dates = [self.start_date_leapyear + timedelta(days=x) for x in range((self.end_date - self.start_date_leapyear).days + 1)]
        expected_files = [f"chirp.{date.strftime('%Y.%m.%d')}.tif" for date in dates]
        for file in expected_files:
            file_path = os.path.join(self.chirps_path, file)
            self.assertTrue(os.path.exists(file_path))
        
        transformed_files = glob.glob(os.path.join(self.chirps_path, '*.tif'))
        self.assertEqual(len(transformed_files), 29)  # 29 days of data downloaded for each variable
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # TEST DOWNLOAD ERA 5
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def test_download_era5_data_single_variable(self):
        # Test downloading era5 data for a single variable
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Perform the download for a single variable (t_max)
        complete_data.download_era5_data(self.daily_downloaded_path, variables=["t_max"])

        # Check if the era5 data files were downloaded, extracted, and transformed
        variable_path = os.path.join(self.era5_path, "t_max")
        self.assertTrue(os.path.exists(variable_path))
        transformed_files = glob.glob(os.path.join(variable_path, '*.tif'))
        self.assertEqual(len(transformed_files), 30)  # 30 days of data downloaded

    def test_download_era5_data_multiple_variables(self):
        # Test downloading era5 data for multiple variables (t_max, t_min, sol_rad)
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Perform the download for multiple variables
        complete_data.download_era5_data(self.daily_downloaded_path, variables=["t_max", "t_min", "sol_rad"])

        # Check if the era5 data files were downloaded, extracted, and transformed for all variables
        variables = ["t_max", "t_min", "sol_rad"]
        for variable in variables:
            variable_path = os.path.join(self.era5_path, variable)
            self.assertTrue(os.path.exists(variable_path))
            transformed_files = glob.glob(os.path.join(variable_path, '*.tif'))
            self.assertEqual(len(transformed_files), 30)  # 3 days of data downloaded for each variable

    def test_download_era5_data_single_variable_leapyear(self):
        # Test downloading era5 data for a single variable
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Perform the download for a single variable (t_max)
        complete_data.download_era5_data(self.daily_downloaded_path, variables=["t_max"])

        # Check if the era5 data files were downloaded, extracted, and transformed
        variable_path = os.path.join(self.era5_path, "t_max")
        self.assertTrue(os.path.exists(variable_path))
        transformed_files = glob.glob(os.path.join(variable_path, '*.tif'))
        self.assertEqual(len(transformed_files), 29)  # 30 days of data downloaded

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # TEST EXTRACT VALUES
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def test_extract_values_single_file_single_location_chirp(self):
        variable = 'prec'
        # Test extracting values for a single file and a single location
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Copy the two raster for testing
        self.create_mock_raster()

        # Perform the extraction
        extracted_data = complete_data.extract_values(self.chirps_path, variable, self.location, -14,-4,'%Y.%m.%d')

        # Check if the extracted data is correct
        expected_data = [{'ws': 'Test Location', 'day': 1, 'month': 6, 'year': 2023, variable: 20.493248}]
        self.assertEqual(extracted_data, expected_data)

    def test_extract_values_single_file_single_location_era5(self):
        variable = self.variable_era5
        # Test extracting values for a single file and a single location
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Copy the two raster for testing
        self.create_mock_raster()

        # Perform the extraction
        extracted_data = complete_data.extract_values(os.path.join(self.era5_data,variable), variable, self.location, -23,-15,'%Y%m%d')

        # Check if the extracted data is correct
        expected_data = [{'ws': 'Test Location', 'day': 1, 'month': 6, 'year': 2023, variable: 20.708344}]

        self.assertEqual(extracted_data, expected_data)

    def test_extract_values_multiple_files_multiple_locations_chirp(self):
        # Test extracting values for multiple files and multiple locations
        variable = 'prec'
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Copy the two raster for testing
        self.create_mock_raster()

        # Perform the extraction
        extracted_data = complete_data.extract_values(self.chirps_path, variable, self.locations, -14,-4,'%Y.%m.%d')

        # Check if the extracted data is correct
        expected_data = [
            {'ws': 'Location 1', 'day': 1, 'month': 6, 'year': 2023, variable: 20.493248},
            {'ws': 'Location 2', 'day': 1, 'month': 6, 'year': 2023, variable: 11.695796}
        ]
        self.assertEqual(extracted_data, expected_data)

    def test_extract_values_multiple_files_multiple_locations_era5(self):
        # Test extracting values for multiple files and multiple locations
        variable = self.variable_era5
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Copy the two raster for testing
        self.create_mock_raster()

        # Perform the extraction
        extracted_data = complete_data.extract_values(os.path.join(self.era5_data,variable), variable, self.locations, -23,-15,'%Y%m%d')

        # Check if the extracted data is correct
        expected_data = [
            {'ws': 'Location 1', 'day': 1, 'month': 6, 'year': 2023, variable: 20.708344},
            {'ws': 'Location 2', 'day': 1, 'month': 6, 'year': 2023, variable: 25.889648}
        ]
        self.assertEqual(extracted_data, expected_data)

    # =-=-=-=-=-=-=-=-=-
    # TEST EXTRACT CHIRP
    # =-=-=-=-=-=-=-=-=-

    def test_extract_chirp_data_single_location(self):
        # Test extracting chirp data for a single location
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Perform the extraction
        extracted_data = complete_data.extract_chirp_data(self.daily_downloaded_path, self.location)

        # Check if the extracted data is correct
        expected_data = pd.DataFrame({
            'ws': ['Test Location'],
            'day': [1],
            'month': [6],
            'year': [2023],
            'prec': [20.493248]
        })
        pd.testing.assert_frame_equal(extracted_data, expected_data)

    def test_extract_chirp_data_multiple_locations(self):
        # Test extracting chirp data for a single location
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Perform the extraction
        extracted_data = complete_data.extract_chirp_data(self.daily_downloaded_path, self.location)

        # Check if the extracted data is correct
        expected_data = pd.DataFrame({
            'ws': ['Test Location'],
            'day': [1,1],
            'month': [6,6],
            'year': [2023,2023],
            'prec': [20.493248,11.695796]
        })
        pd.testing.assert_frame_equal(extracted_data, expected_data)

    # =-=-=-=-=-=-=-=-=-
    # TEST EXTRACT ERA 5
    # =-=-=-=-=-=-=-=-=-

    def test_extract_era5_data_multiple_locations_single_variable(self):
        # Test extracting era5 data for multiple locations and a single variable
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Perform the extraction for t_max variable
        extracted_data = complete_data.extract_era5_data(self.daily_downloaded_path, self.location, variables=[self.variable_era5])

        # Check if the extracted data is correct
        expected_data = pd.DataFrame({
            'ws': ['Location 1', 'Location 2'],
            'day': [1, 1],
            'month': [6, 6],
            'year': [2023, 2023],
            't_max': [20.708344, 25.889648]
        })

        pd.testing.assert_frame_equal(extracted_data, expected_data)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-
    # TEST EXTRACT CLIMATOLOGY
    # =-=-=-=-=-=-=-=-=-=-=-=-=-
if __name__ == '__main__':
    unittest.main()