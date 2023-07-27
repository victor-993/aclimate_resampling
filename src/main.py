
# -*- coding: utf-8 -*-
# Run all the functions together
# Created by: Maria Victoria Diaz
# Alliance Bioversity, CIAT. 2023

from funciones_aclimate import *


def resampling_master(station, input_root, climate_data_root, proba, output_root, year_forecast, forecast_period = 3):

    import pandas as pd
    import calendar
    import numpy as np
    import random
    import os
    import warnings
    warnings.filterwarnings("ignore")

    if os.path.exists(output_root):
        output_root = output_root
    else:
        os.mkdir(output_root)

    print("Fixing issues in the databases")
    verifica = mdl_verification(ruta_daily_data, ruta_probabilidades)



    print("Reading the probability file and getting the forecast seasons")
    prob_normalized = preprocessing(input_root, verifica, output_root, forecast_period)



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

    if len(resampling_forecast) == 3:
        return resampling_forecast[2]
    else:
        return None
    

import datetime

import pandas as pd

from complete_data import CompleteData


if __name__ == "__main__":
    print("Setting global parameters")
    start_date = (datetime.date.today() - pd.DateOffset(months=1)).replace(day=1)
    country = "ETHIOPIA"
    #country = "TANZANIA"
    path = "D:\\CIAT\\Code\\USAID\\aclimate_resampling\\data\\"
    #path = "C:\\temp\\test\\"
    ##region = [14,32,3,48] # North, West, South, East
    ##region = [0,28,-12,41] # North, West, South, East
    cores = 2
    #dd = CompleteData(start_date,country,path,region,cores=cores)
    dd = CompleteData(start_date,country,path,cores=cores)
    dd.run()
