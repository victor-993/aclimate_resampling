import sys
import datetime
import pandas as pd
import warnings

from dateutil.relativedelta import relativedelta

from aclimate_resampling import AClimateResampling
from complete_data import CompleteData


if __name__ == "__main__":
    # Params
    # 0: Country
    # 1: Path root
    # 2: Previous months
    # 3: Cores
    parameters = sys.argv[2:]
    print("Reading inputs")
    #country = "ETHIOPIA"
    country = parameters[0]
    #path = "D:\\CIAT\\Code\\USAID\\aclimate_resampling\\data\\"
    path = parameters[1]
    #start_date = (datetime.date.today() - pd.DateOffset(months=1)).replace(day=1)
    months_previous = int(parameters[2])
    start_date = (datetime.date.today() - pd.DateOffset(months=months_previous)).replace(day=1)
    cores = int(parameters[3])
    
    ar = AClimateResampling(path, country, cores=cores, year_forecast = year_forecast)
    ar.resampling_master()
    dd = CompleteData(start_date,country,path,cores=cores)
    dd.run()

#python resampling.py "ETHIOPIA" "D:\\CIAT\\Code\\USAID\\aclimate_resampling\\data\\" "-1" 2