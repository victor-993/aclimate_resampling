import sys
import os
import shutil
import glob
from zipfile import ZipFile

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
        self.start_date_leapyear = datetime(2020, 2, 1) # Leap year
        self.end_date = datetime(2023, 6, 3)  # Only 3 days for this test
        self.cores = 2
        self.country = 'ETHIOPIA'
        self.variable_era5 = "t_max"
        self.variables_era5 = ["t_max","t_min"]
        self.chirp_data = "chirp.2023.06.01.tif"
        self.era5_data = "Temperature-Air-2m-Max-24h_C3S-glob-agric_AgERA5_20230601_final-v1.0.tif"
        self.chirps_url_name = f"chirp.{self.start_date.strftime('%Y.%m.%d')}.tif.gz"
        self.chirps_file_name = f"chirp.{self.start_date.strftime('%Y.%m.%d')}.tif"
        self.url = f"http://data.chc.ucsb.edu/products/CHIRP/daily/{str(self.start_date.year)}/{self.chirps_url_name}"
        self.location = pd.DataFrame({'ws': ['Test Location'], 'lat': [6.4095], 'lon': [-72.0211]})
        self.locations = pd.DataFrame({ 'ws': ['Location 1', 'Location 2'], 'lat': [6.4095, 6.3830], 'lon': [-72.0211, -71.8700]})

        #self.data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        #self.root_data = os.path.abspath(os.path.join(os.path.dirname(__file__), 'test_files'))

        self.path_data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

        self.path_env = os.path.abspath(os.path.join(os.path.dirname(__file__), 'test_files'))
        self.path_env_country = os.path.join(self.path_env,self.country)
        self.path_env_country_inputs = os.path.join(self.path_env_country,"inputs")
        self.path_env_country_inputs_forecast = os.path.join(self.path_env_country_inputs,"prediccionClimatica")
        self.path_env_country_inputs_forecast_dailydata = os.path.join(self.path_env_country_inputs_forecast,"dailyData")
        self.path_env_country_inputs_forecast_dailydownloaded = os.path.join(self.path_env_country_inputs_forecast,"daily_downloaded")
        self.path_env_country_inputs_forecast_dailydownloaded_chirp = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded,"chirp")
        self.path_env_country_inputs_forecast_dailydownloaded_era5 = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded,"era5")

        self.chirps_file_path = os.path.join(self.path_env_country, self.chirps_file_name)
        self.chirps_file_path_compressed = os.path.join(self.path_env_country, self.chirps_url_name)

        os.makedirs(self.path_env_country, exist_ok=True)

        """
        self.inputs = os.path.join(self.root_data,self.country,"inputs")
        self.inputs_prediccion = os.path.join(self.inputs,"prediccionClimatica")
        self.daily_downloaded_path = os.path.join(self.inputs_prediccion, "daily_downloaded")
        self.daily_data_path = os.path.join(self.inputs_prediccion, "dailyData")
        self.chirps_path = os.path.join(self.daily_downloaded_path, "chirps")
        self.chirps_file_path = os.path.join(self.chirps_path, self.chirps_file_name)
        self.chirps_file_path_compressed = os.path.join(self.chirps_path, self.chirps_url_name)
        self.era5_path = os.path.join(self.daily_downloaded_path, "era5")

        self.outputs = os.path.join(self.root_data,self.country,"outputs")
        self.outputs_resampling = os.path.join(self.outputs,"resampling")
        
        os.makedirs(self.inputs_prediccion, exist_ok=True)
        os.makedirs(self.chirps_path, exist_ok=True)
        #os.makedirs(self.outputs, exist_ok=True)

        os.makedirs(os.path.join(self.era5_path,self.variable_era5), exist_ok=True)
        if not os.listdir(self.daily_data_path):
            shutil.copytree(os.path.join(self.data,"inputs","prediccionClimatica","dailyData"),self.daily_data_path, ignore=shutil.ignore_patterns('*'))
        if not os.listdir(self.outputs_resampling):
            shutil.copytree(os.path.join(self.data,"outputs"),self.outputs, ignore=shutil.ignore_patterns('*'))
        #shutil.copytree(self.data,self.inputs)
        #shutil.copytree(self.data,self.outputs_resampling)
        """

    def tearDown(self):
        # Clean up the temporary test directory and its contents after each test
        shutil.rmtree(self.path_env)
        pass
    
    def create_mock_raster(self):
        chirp_src = os.path.join(self.path_data,self.chirp_data)
        era5_src = os.path.join(self.path_data,self.era5_data)
        chirp_dst = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_chirp,self.chirp_data)
        era5_dst = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_era5,self.variable_era5,self.era5_data)

        with ZipFile(chirp_src + ".zip", 'r') as zObject:
            zObject.extractall(path=self.path_data)
        with ZipFile(era5_src + ".zip", 'r') as zObject:
            zObject.extractall(path=self.path_data)

        if not(os.path.exists(chirp_dst)):
            shutil.move(chirp_src, chirp_dst)
        if not(os.path.exists(era5_dst)):
            shutil.move(era5_src, era5_dst)
    
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # TEST DOWNLOAD FILE
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def test_download_file_force_false(self):
        # Test downloading a file with force=False (file already exists)
        complete_data = CompleteData(self.start_date, self.country, self.path_env, cores=self.cores)

        # Create an empty file to simulate an already downloaded file
        if not(os.path.exists(self.chirps_file_path)):
            #os.makedirs(self.chirps_path, exist_ok=True)
            open(self.chirps_file_path, 'w').close()

        # Perform the download with force=False
        complete_data.download_file(self.url, self.chirps_file_path_compressed, force=False)

        # Ensure the file was not downloaded again
        self.assertTrue(os.path.exists(self.chirps_file_path))

    def test_download_file_force_true(self):
        # Test downloading a file with force=True
        complete_data = CompleteData(self.start_date, self.country, self.path_env, cores=self.cores)
        
        # Create an empty file to simulate an already downloaded file
        if not(os.path.exists(self.chirps_file_path)):
            #os.makedirs(self.chirps_path, exist_ok=True)
            open(self.chirps_file_path, 'w').close()
        # Ensure the file exist before downloading
        self.assertTrue(os.path.exists(self.chirps_file_path))
        
        # Perform the download with force=True
        complete_data.download_file(self.url, self.chirps_file_path_compressed, force=True)
        
        # Check if the file was downloaded and extracted
        self.assertTrue(os.path.exists(self.chirps_file_path))

    def test_download_file_path_not_exists(self):
        # Test downloading a file to a path that does not exist
        complete_data = CompleteData(self.start_date, self.country, self.path_env, cores=self.cores)
        
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
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Perform the download
        complete_data.download_data_chirp()

        # Check if the chirp data files were downloaded and stored in the correct location
        dates = [self.start_date + timedelta(days=x) for x in range((self.end_date - self.start_date).days + 1)]
        expected_files = [f"chirp.{date.strftime('%Y.%m.%d')}.tif" for date in dates]
        for file in expected_files:
            file_path = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_chirp, file)
            self.assertTrue(os.path.exists(file_path))

    def test_download_data_chirp_existing_files(self):
        # Test downloading chirp data when some files already exist and force=False
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Create some mock chirp data files
        for date in [self.start_date + timedelta(days=1), self.start_date + timedelta(days=2)]:
            file_name = f"chirp.{date.strftime('%Y.%m.%d')}.tif"
            file_path = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_chirp, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            if os.path.exists(file_path):
                os.remove(file_path)
            with open(file_path, 'w') as f:
                f.write("Mock data")

        # Perform the download
        complete_data.download_data_chirp()

        # Check if the existing files were not downloaded again
        for date in [self.start_date + timedelta(days=1), self.start_date + timedelta(days=2)]:
            file_name = f"chirp.{date.strftime('%Y.%m.%d')}.tif"
            file_path = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_chirp, file_name)
            self.assertTrue(os.path.exists(file_path))
            self.assertEqual(os.path.getsize(file_path), 9)  # Size of the "Mock data"

    def test_download_data_chirp_leapyear(self):
        # Test downloading chirp data for a leap year
        complete_data = CompleteData(start_date=self.start_date_leapyear, country=self.country, path=self.path_env, cores=self.cores)

        # Perform the download
        complete_data.download_data_chirp()

        # Check if the chirp data files were downloaded and stored in the correct location
        transformed_files = glob.glob(os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_chirp, '*.tif'))
        self.assertEqual(len(transformed_files), 29)  # 29 days of data downloaded for each variable

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # TEST DOWNLOAD ERA 5
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def test_download_era5_data_single_variable(self):
        # Test downloading era5 data for a single variable
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Perform the download for a single variable (t_max)
        complete_data.download_era5_data(variables=[self.variable_era5])

        # Check if the era5 data files were downloaded, extracted, and transformed
        variable_path = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_era5, self.variable_era5)
        self.assertTrue(os.path.exists(variable_path))
        transformed_files = glob.glob(os.path.join(variable_path, '*.tif'))
        self.assertEqual(len(transformed_files), 30)  # 30 days of data downloaded

    def test_download_era5_data_multiple_variables(self):
        # Test downloading era5 data for multiple variables (t_max, t_min, sol_rad)
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Perform the download for multiple variables
        complete_data.download_era5_data(variables=self.variables_era5)

        # Check if the era5 data files were downloaded, extracted, and transformed for all variables
        
        for variable in self.variables_era5:
            variable_path = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_era5, variable)
            self.assertTrue(os.path.exists(variable_path))
            transformed_files = glob.glob(os.path.join(variable_path, '*.tif'))
            self.assertEqual(len(transformed_files), 30)  # 3 days of data downloaded for each variable

    def test_download_era5_data_single_variable_leapyear(self):
        # Test downloading era5 data for a single variable
        complete_data = CompleteData(start_date=self.start_date_leapyear, country=self.country, path=self.path_env, cores=self.cores)

        # Perform the download for a single variable (sol_rad)
        complete_data.download_era5_data(variables=["sol_rad"])

        # Check if the era5 data files were downloaded, extracted, and transformed
        variable_path = os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_era5, "sol_rad")
        self.assertTrue(os.path.exists(variable_path))
        transformed_files = glob.glob(os.path.join(variable_path, '*.tif'))
        self.assertEqual(len(transformed_files), 29)  # 29 days of data downloaded

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # TEST EXTRACT VALUES
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    
    def test_extract_values_single_location_chirp(self):
        variable = 'prec'
        # Test extracting values for a single file and a single location
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Copy the two raster for testing
        self.create_mock_raster()

        # Perform the extraction
        extracted_data = complete_data.extract_values(self.path_env_country_inputs_forecast_dailydownloaded_chirp, variable, self.location, -14,-4,'%Y.%m.%d')

        # Check if the extracted data is correct
        expected_data = [{'ws': 'Test Location', 'day': 1, 'month': 6, 'year': 2023, variable: 20.493248}]

        self.assertEqual(extracted_data[0]['ws'], expected_data[0]['ws'])
        self.assertEqual(extracted_data[0]['day'], expected_data[0]['day'])
        self.assertEqual(extracted_data[0]['month'], expected_data[0]['month'])
        self.assertEqual(extracted_data[0]['year'], expected_data[0]['year'])
        self.assertEqual(int(extracted_data[0][variable]),int(expected_data[0][variable]))

    def test_extract_values_multiple_locations_chirp(self):
        # Test extracting values for multiple files and multiple locations
        variable = 'prec'
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Copy the two raster for testing
        self.create_mock_raster()

        # Perform the extraction
        extracted_data = complete_data.extract_values(self.path_env_country_inputs_forecast_dailydownloaded_chirp, variable, self.locations, -14,-4,'%Y.%m.%d')

        # Check if the extracted data is correct
        expected_data = [
            {'ws': 'Location 1', 'day': 1, 'month': 6, 'year': 2023, variable: 20.493248},
            {'ws': 'Location 2', 'day': 1, 'month': 6, 'year': 2023, variable: 11.695796}
        ]
        for i in [0,1]:
            self.assertEqual(extracted_data[i]['ws'], expected_data[i]['ws'])
            self.assertEqual(extracted_data[i]['day'], expected_data[i]['day'])
            self.assertEqual(extracted_data[i]['month'], expected_data[i]['month'])
            self.assertEqual(extracted_data[i]['year'], expected_data[i]['year'])
            self.assertEqual(int(extracted_data[i][variable]),int(expected_data[i][variable]))
    
    def test_extract_values_single_file_single_location_era5(self):
        variable = self.variable_era5
        # Test extracting values for a single file and a single location
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Copy the two raster for testing
        self.create_mock_raster()

        # Perform the extraction
        extracted_data = complete_data.extract_values(os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_era5,variable), variable, self.location, -23,-15,'%Y%m%d')

        # Check if the extracted data is correct
        expected_data = []
        expected_data.append({'ws': 'Test Location', 'day': 1, 'month': 6, 'year': 2023, variable: 20.708344})
        self.assertEqual(extracted_data[0]['ws'], expected_data[0]['ws'])
        self.assertEqual(extracted_data[0]['day'], expected_data[0]['day'])
        self.assertEqual(extracted_data[0]['month'], expected_data[0]['month'])
        self.assertEqual(extracted_data[0]['year'], expected_data[0]['year'])
        self.assertEqual(int(extracted_data[0][variable]),int(expected_data[0][variable]))

    def test_extract_values_multiple_files_multiple_locations_era5(self):
        # Test extracting values for multiple files and multiple locations
        variable = self.variable_era5
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Copy the two raster for testing
        self.create_mock_raster()

        # Perform the extraction
        extracted_data = complete_data.extract_values(os.path.join(self.path_env_country_inputs_forecast_dailydownloaded_era5,variable), variable, self.locations, -23,-15,'%Y%m%d')

        # Check if the extracted data is correct
        expected_data = [
            {'ws': 'Location 1', 'day': 1, 'month': 6, 'year': 2023, variable: 20.708344},
            {'ws': 'Location 2', 'day': 1, 'month': 6, 'year': 2023, variable: 25.889648}
        ]
        for i in [0,1]:
            self.assertEqual(extracted_data[i]['ws'], expected_data[i]['ws'])
            self.assertEqual(extracted_data[i]['day'], expected_data[i]['day'])
            self.assertEqual(extracted_data[i]['month'], expected_data[i]['month'])
            self.assertEqual(extracted_data[i]['year'], expected_data[i]['year'])
            self.assertEqual(int(extracted_data[i][variable]),int(expected_data[i][variable]))
    
    # =-=-=-=-=-=-=-=-=-
    # TEST EXTRACT CHIRP
    # =-=-=-=-=-=-=-=-=-
    
    def test_extract_chirp_data_single_location(self):
        # Test extracting chirp data for a single location
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Perform the extraction
        extracted_data = complete_data.extract_chirp_data(self.location)

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
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Perform the extraction
        extracted_data = complete_data.extract_chirp_data(self.locations)

        # Check if the extracted data is correct
        expected_data = pd.DataFrame({
            'ws': ['Location 1','Location 2'],
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
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Perform the extraction for t_max variable
        extracted_data = complete_data.extract_era5_data(self.locations, variables=[self.variable_era5])
        extracted_data = extracted_data.loc[extracted_data["day"] == 1,:]
        
        # Check if the extracted data is correct
        expected_data = pd.DataFrame({
            'ws': ['Location 1', 'Location 2'],
            'day': [1, 1],
            'month': [6, 6],
            'year': [2023, 2023],
            't_max': [20.708344, 25.889648]
        })
        expected_data[self.variable_era5] = expected_data[self.variable_era5].astype('float32')

        pd.testing.assert_frame_equal(extracted_data, expected_data)
    
    # =-=-=-=-=-=-=-=-=-=-=-=-=-
    # TEST LIST WEATHER STATION
    # =-=-=-=-=-=-=-=-=-=-=-=-=-

    def test_list_ws_stations(self):
        # Test listing stations with a single valid station
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Perform listing of stations
        df_ws = complete_data.list_ws()

        # Create a mock coordinates file for a single station
        ws_names = ['5e91e1c214daf81260ebba59', '5eb346bdebd0050e38685f3e', '5ebad0a74c06b707e80d5c4a']
        lats = [12.79, 12.13649, 10.057273]
        lons = [39.65, 39.66048, 34.54025]
        msgs = ['','','']

        # Check if the station information is correctly listed
        expected_data = pd.DataFrame({
            'ws': ws_names,
            'lat': lats,
            'lon': lons,
            'message': msgs
        })
        expected_data["ws"] = expected_data["ws"].astype('string')
        expected_data["message"] = expected_data["message"].astype('string')

        pd.testing.assert_frame_equal(df_ws, expected_data)

    def test_list_ws_stations_without_coords(self):
        # Test listing stations with a single valid station
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.path_env, cores=self.cores)

        # Remove one coords file for one station
        if os.path.exists(os.path.join(self.path_env_country_inputs_forecast_dailydata,"5ebad0a74c06b707e80d5c4a_coords.csv")):
            shutil.rmtree(os.path.join(self.path_env_country_inputs_forecast_dailydata,"5ebad0a74c06b707e80d5c4a_coords.csv"))

        # Perform listing of stations
        df_ws = complete_data.list_ws()

        # Create a mock coordinates file for a single station
        ws_names = ['5e91e1c214daf81260ebba59', '5eb346bdebd0050e38685f3e', '5ebad0a74c06b707e80d5c4a']
        lats = [12.79, 12.13649, 10.057273]
        lons = [39.65, 39.66048, 34.54025]
        messages = ['', '', 'ERROR with coordinates']

        # Check if the station information is correctly listed
        expected_data = pd.DataFrame({
            'ws': [ws_names],
            'lat': [lats],
            'lon': [lons],
            'message': [messages]
        })
        expected_data["ws"] = expected_data["ws"].astype('string')
        expected_data["message"] = expected_data["message"].astype('string')

        pd.testing.assert_frame_equal(df_ws, expected_data)
    
    # =-=-=-=-=-=-=-=-=-=-=-=-=-
    # TEST EXTRACT CLIMATOLOGY
    # =-=-=-=-=-=-=-=-=-=-=-=-=-
    """
    def test_extract_climatology_single_location(self):
        # Test extracting climatology for a single location
        complete_data = CompleteData(start_date=self.start_date, country=self.country, path=self.root_data, cores=self.cores)

        # Create a mock climatology data file for a single location
        location_name = 'Location 1'
        data = pd.DataFrame({
            'ws': [location_name] * 12,
            'day': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'month': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'year': [2023] * 12,
            'prec': [10, 20, 15, 30, 25, 35, 40, 45, 50, 55, 60, 65],
            't_max': [30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85],
            't_min': [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65],
            'sol_rad': [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200]
        })
        self.create_mock_climatology_data(location_name, data)

        # Create a mock locations DataFrame with a single location
        locations = pd.DataFrame({
            'ws': [location_name],
            'lat': [40.0],
            'lon': [-120.0]
        })

        # Perform the extraction
        extracted_data = complete_data.extract_climatology(self.daily_downloaded_path, locations)

        # Check if the extracted data is correct
        expected_data = data.loc[data['month'] == 7].copy()
        expected_data["year"] = 2023
        expected_data = expected_data[['ws', 'day', 'month', 'year', 'prec', 't_max', 't_min', 'sol_rad']]
        pd.testing.assert_frame_equal(extracted_data, expected_data)

    def test_extract_climatology_multiple_locations(self):
        # Test extracting climatology for multiple locations
        start_date = datetime.datetime(2023, 7, 1)
        country = 'US'
        path = self.test_path
        complete_data = CompleteData(start_date=start_date, country=country, path=path, cores=1, force=False)

        # Create mock climatology data files for multiple locations
        location1_data = pd.DataFrame({
            'ws': ['Location 1'] * 12,
            'day': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'month': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'year': [2023] * 12,
            'prec': [10, 20, 15, 30, 25, 35, 40, 45, 50, 55, 60, 65],
            't_max': [30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85],
            't_min': [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65],
            'sol_rad': [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200]
        })
        self.create_mock_climatology_data('Location 1', location1_data)

        location2_data = pd.DataFrame({
            'ws': ['Location 2'] * 12,
            'day': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'month': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'year': [2023] * 12,
            'prec': [20, 30, 25, 40, 35, 45, 50, 55, 60, 65, 70, 75],
            't_max': [35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90],
            't_min': [15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
            'sol_rad': [200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300]
        })
        self.create_mock_climatology_data('Location 2', location2_data)

        # Create a mock locations DataFrame with multiple locations
        locations = pd.DataFrame({
            'ws': ['Location 1', 'Location 2'],
            'lat': [40.0, 41.0],
            'lon': [-120.0, -121.0]
        })
    
    """
if __name__ == '__main__':
    unittest.main()