import datetime

import pandas as pd

from src.download_data import DownloadData


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
    #dd = DownloadData(start_date,country,path,region,cores=cores)
    dd = DownloadData(start_date,country,path,cores=cores)
    dd.run()