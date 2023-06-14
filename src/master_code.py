
# -*- coding: utf-8 -*-
# Run all the functions together
# Created by: Maria Victoria Diaz
# Alliance Bioversity, CIAT. 2023




def resampling_master (station, input_root, climate_data_root, output_root, year_forecast, forecast_period = 3):

    import funciones_aclimate
    import pandas as pd
    import calendar
    import numpy as np
    import random
    import os
    import warnings
    warnings.filterwarnings("ignore")


    
    print("Reading the probability file and getting the forecast seasons")
    prob_normalized = processing(input_root, output_root, forecast_period)



    print("Resampling and creating the forecast scenaries")
    resampling_forecast = forecast_station(station = station, 
                                           prob = prob_normalized, 
                                           daily_data_root = climate_data_root, 
                                           output_root = output_root, 
                                           year_forecast = year_forecast, 
                                           forecast_period= forecast_period)
    


    print("Saving escenaries and a summary")
    save_forecast(output_root = output_root, 
                  year_forecast = year_forecast,
                  forecast_period = forecast_period, 
                  prob = prob_normalized, 
                  base_years = resampling_forecast[0], 
                  seasons_range = resampling_forecast[1], 
                  station = station)