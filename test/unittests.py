
import unittest
import pandas as pd
import os
import numpy as np

from funciones_aclimate import preprocessing, forecast_station, save_forecast

class ForecastStationTests(unittest.TestCase):
    def setUp(self):
        # Define sample input data for testing
        self.station = "62a74f9bea81f11fe450cbc4"
        self.daily_data_root = "/content/drive/MyDrive/ANGOLA/inputs/prediccionClimatica/dailyData"
        self.output_root = "/content/drive/MyDrive/ANGOLA/save"


### check negative

    def test_negative_data_in_prob(self):
    # Create a sample prob DataFrame with negative values
        prob = pd.DataFrame({'id': ['station1', 'station2'],
                         'Season': ['season1', 'season2'],
                         'Start': [1, 2],
                         'End': [2, 3],
                         'Type': ['above', 'normal'],
                         'Prob': [0.5, -0.2]})
        forecast_period = 'bi'
    # Call the processing function and expect it to raise a ValueError
        with self.assertRaises(ValueError):
          preprocessing(prob,  self.output_root, forecast_period)


### check if outputs are generated correctly

    def test_output_generation(self):
        prob = pd.DataFrame({'id': [self.station],
                         'Season': ['season1', 'season2'],
                         'Start': [1,2],
                         'End': [2,3],
                         'Type': ['above', 'below'],
                         'Prob': [0.5, 0.5]})
    
        year_forecast = 2023
        forecast_period = 'bi'

        base_years, seasons_range = forecast_station(self.station, prob, self.daily_data_root, self.output_root, year_forecast, forecast_period)
    
    # Check if the output folder exists
        self.assertTrue(os.path.exists(self.output_root))
    
    # Check if the station folder exists within the output folder
        self.assertTrue(os.path.exists(os.path.join(self.output_root, self.station)))
    
    # Check if the correct subfolder ('bi') or ('tri') is created within the station folder
        forecast_folder = 'bi' if forecast_period == 'bi' else 'tri'
        self.assertTrue(os.path.exists(os.path.join(self.output_root, self.station, forecast_folder)))
    
    # Check if the output files are generated with the correct columns
        expected_samples_columns = ['id', 'season1', 'season2']
        expected_range_columns = ['day',	'month', 'year', 't_max', 't_min', 'prec', 'sol_rad', 'Season', 'id']
        self.assertEqual(list(base_years.columns), expected_samples_columns)
        self.assertEqual(list(seasons_range.columns), expected_range_columns)
    
    # Cleanup: Remove the test output folder
        os.removedirs(self.output_root)


if __name__ == '__main__':
    unittest.main()

























