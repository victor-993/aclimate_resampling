import os
import glob
import datetime
import json

import datetime
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor

import requests

import pandas as pd
import numpy as np

import urllib.request
from tqdm import tqdm

import rasterio

import pickle

import cdsapi
#import xarray as xr
import pandas as pd


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Function which creates a folder. It checks if the folders exist before
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# (string) path: Path where the folder should be create
def mkdir(path):
    if not os.path.exists(path): 
        os.mkdir(path)

def download_file(url, path, force = False):
    if force and os.path.exists(path) == False:
        with DownloadProgressBar(unit='B', unit_scale=True,miniters=1, desc=url.split('/')[-1]) as t:
            urllib.request.urlretrieve(url, filename=path, reporthook=t.update_to)
    else:
        print("File already downloaded!",path)

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Function to download chirps data
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# save_path: path to save raster files.
# start_date: start date to download.
# end_date: end date to download.
# year_to: resampling year.
# cores: # cores to use in parallel.
# force: If you want to force to execute the process
# OUTPUT: save raster layers.
def download_data_chirp(save_path, start_date, end_date, year_to, cores, force = False):
    save_path_chirp = os.path.join(save_path,"chirps")
    mkdir(save_path_chirp)

    dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    urls = [f"http://data.chc.ucsb.edu/products/CHIRP/daily/{year_to}/chirp.{date.strftime('%Y.%m.%d')}.tif" for date in dates]
    files = [os.path.basename(url) for url in urls]
    save_path_chirp_all = [os.path.join(save_path_chirp, file) for file in files]
    force_all = [force] * len(files)

    with ThreadPoolExecutor(max_workers=cores) as executor:
        executor.map(download_file, urls, save_path_chirp_all,force_all)

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Function to download ERA 5 data
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# save_path: path to save raster files.
# start_date: start date to download.
# end_date: end date to download.
# # region: North, West, South, East
# variables: List of variables to download. by default
# cores: # cores to use in parallel.
# OUTPUT: save raster layers.
def download_era5_data(save_path, start_date, end_date, region, variables=["2m_temperature"],):
    save_path_era5 = os.path.join(save_path,"era5")
    mkdir(save_path_era5)
    save_path_era5 = os.path.join(save_path,"era.nc")

    c = cdsapi.Client()

    c.retrieve('reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': variables,
            'area': f'{region[0]}/{region[1]}/{region[2]}/{region[3]}',
            'date': f'{start_date.strftime("%Y%m%d")}/{end_date.strftime("%Y%m%d")}',
        },
        save_path_era5
    )


# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# 1. Function to extract NASA POWER daily data 
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# This function 
# INPUT
# * data : station data. 
# * special_data: this contains (lat: latitude of the station / site of interest, 
# lon: longitud of the station / site of interest, year_to: actual year, month_to: actual month).

# OUTPUT: NASA series. 
# It could be possible NASA API in some case sometimes don't work. 


## up 2021 jre: Se modifica esta funcion para la version 2.0 de la API 
## 

def download_data_nasa(ini_date, end_date, station_data, special_data):
    lat = special_data["lat"]
    lon = special_data["lon"]
    year_to = ini_date.year
    month_to = ini_date.month

    json_file = f"https://power.larc.nasa.gov/api/temporal/daily/point?start=20150101&end={end_date.strftime('%Y%m%d')}&latitude={lat}&longitude={lon}&community=ag&parameters=ALLSKY_SFC_SW_DWN,T2M_MAX,T2M_MIN&header=true&time-standard=lst"
    print(json_file)

    response = requests.get(json_file)
    json_data = json.loads(response.text)

    data_nasa = pd.concat([pd.DataFrame(parameter).assign(parameter_name=name) for name, parameter in json_data['properties']['parameter'].items()])
    data_nasa = data_nasa.pivot(index='date', columns='parameter_name', values='value')
    data_nasa['date'] = pd.to_datetime(data_nasa.index)
    data_nasa['year_n'] = data_nasa['date'].dt.year
    data_nasa['month'] = data_nasa['date'].dt.month
    data_nasa['day'] = data_nasa['date'].dt.day
    data_nasa = data_nasa.rename(columns={"ALLSKY_SFC_SW_DWN": "sol_rad", "T2M_MAX": "t_max", "T2M_MIN": "t_min"})
    data_nasa = data_nasa[['date', 'year_n', 'month', 'day', 't_min', 't_max', 'sol_rad']].replace(-999, pd.NA)

    all_data = pd.merge(station_data[station_data['year'].isin(data_nasa['year_n'].unique())].drop(columns='prec'),
                        data_nasa[data_nasa['year_n'].isin(station_data['year'].unique())].rename(columns={'date': 'dates', 'year_n': 'year', 'month': 'month', 'day': 'day', 't_min': 't_min_N', 't_max': 't_max_N', 'sol_rad': 'sol_rad_N'}),
                        how='right', on=['dates', 'year', 'month', 'day'])

    mean_less = all_data[['t_max', 't_max_N', 't_min', 't_min_N', 'sol_rad', 'sol_rad_N']].mean()

    nasa_data_dw = data_nasa[(data_nasa['year_n'] == year_to) & (data_nasa['month'] == month_to)].copy()
    nasa_data_dw['t_max'] += mean_less['t_max'] - mean_less['t_max_N']
    nasa_data_dw['t_min'] += mean_less['t_min'] - mean_less['t_min_N']
    nasa_data_dw['sol_rad'] += mean_less['sol_rad'] - mean_less['sol_rad_N']
    nasa_data_dw['t_max'].fillna(nasa_data_dw['t_max'].mean(), inplace=True)
    nasa_data_dw['t_min'].fillna(nasa_data_dw['t_min'].mean(), inplace=True)
    nasa_data_dw['sol_rad'].fillna(nasa_data_dw['sol_rad'].mean(), inplace=True)

    return nasa_data_dw


# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# 2. Function to extract Chirp data and Join with NASA POWER DATA. 
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# ***** INPUT 
# * data:  path for save files.
# * special_data: data from Chirp and nasa for each station.
# *****  OUTPUT
#  This return resampling scenaries join with satellite data. 
#
# ***** Note: This function save files.



### up 2021, jre: se dividio esta funcion en 2, una primera para obtener los datos de chirp y otro para realizar el join de datos finales
def extract_chirp_data(path_chirp, special_data):
    files = [f for f in os.listdir(path_chirp) if f.endswith('.tif')]
    data = []
    
    for file in files:
        file_path = os.path.join(path_chirp, file)
        with rasterio.open(file_path) as dataset:
            lon, lat = special_data['lon'], special_data['lat']
            row, col = dataset.index(lon, lat)
            value = dataset.read(1)[row, col]
            date_str = file[-10:-4]
            date = datetime.datetime.strptime(date_str, '%Y%m%d')
            data.append({'date': date, 'prec': value})
    
    df = pd.DataFrame(data)
    df['day'] = df['date'].dt.day
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df = df[['day', 'month', 'year', 'prec']]
    
    return df

## Funcion para crear mes previo promedio a partir de los datos historicos
## Mes previo  = month(Sys.month) - 1

###day month  year t_max t_min  prec sol_rad
def clim_extract(wth_data, ini_date):
    wth_data = wth_data.groupby(['day', 'month']).agg({
        't_max': 'mean',
        't_min': 'mean',
        'prec': 'median',
        'sol_rad': 'mean'
    }).reset_index()
    
    wth_data['year'] = ini_date.year
    wth_data = wth_data[wth_data['month'] == ini_date.month]
    wth_data = wth_data[['day', 'month', 'year', 't_max', 't_min', 'prec', 'sol_rad']]
    
    return wth_data


# Funcion para unir bases de datos y extraer escenarios finales
# Escenaries <- all_wth_data$Escenaries[[1]]#$data[[1]]$data[[1]] 
# chirp_data <- all_wth_data$chirp_data[[1]]
# nasa_data <- all_wth_data$nasa_data[[1]]
# climatology_mean <- all_wth_data$climatology_mean[[1]]

def join_wth_escenarios(Escenaries, chirp_data, nasa_data, climatology_mean):
    if chirp_data is None:
        prev_month = climatology_mean.copy()
    elif nasa_data is None:
        prev_month = climatology_mean.merge(chirp_data, on=['day', 'month', 'year'], how='left')
        prev_month['prec'] = np.where(prev_month['prec_y'].isna(), prev_month['prec_x'], prev_month['prec_y'])
        prev_month = prev_month[['day', 'month', 'year', 't_max', 't_min', 'prec', 'sol_rad']]
    else:
        prev_month = nasa_data.merge(chirp_data, on=['day', 'month', 'year'], how='left')
        prev_month = prev_month[['day', 'month', 'year', 't_max', 't_min', 'prec', 'sol_rad']]
    
    Escenaries['data'] = Escenaries['data'].apply(lambda x: x.assign(data=x['data'].apply(lambda y: pd.concat([prev_month, y], ignore_index=True))))
    
    return Escenaries

# Function to save scenarios
def write_scenarios(current_index=None):
    current_index = 1 if current_index is None else current_index

    # Load wth_escenaries from pickle file if it exists
    if "wth_escenaries" in locals():
        with open(f"/forecast/workdir/{currentCountry}_wth_scenaries.pkl", "rb") as f:
            wth_escenaries = pickle.load(f)
    else:
        wth_escenaries = pd.read_pickle(f"/forecast/workdir/{currentCountry}_wth_scenaries.pkl")

    length_wth = len(wth_escenaries)
    for current_index in range(current_index, length_wth, 100):
        sub_wth_escenaries = wth_escenaries.iloc[current_index:min(current_index + 99, length_wth)]

        for id_, wth_final in zip(sub_wth_escenaries['id'], sub_wth_escenaries['wth_final']):
            function_to_save(id_, wth_final, path_output)

        print(f"{current_index}-{min(current_index + 99, length_wth)} scenarios written")


def run(path_output,country, region, cores = 1, force = False):
    ## up 2021, jre: Se estandariza el uso de formato fecha, se requiere paquete lubridate
    print("Calculating dates for the process")
    start_date = (datetime.date.today() - pd.DateOffset(months=1)).replace(day=1)
    end_date = (start_date + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
    year_to = start_date.year
    month_to = start_date.month
    print("Init:",start_date,"End:",end_date,"Year:",year_to,"Month:",month_to)

    # Creating folder
    print("Creating the output folder for country")
    path_country = os.path.join(path_output,country) 
    mkdir(path_country)

    # Download Chirps data
    print("CHIRPS data started!")
    download_data_chirp(path_country, start_date, end_date, year_to, cores, force)
    print("CHIRPS data downloaded!")

    # Download ERA 5 data
    print("ERA 5 data started!")
    download_era5_data(path_country, start_date, end_date, region )
    print("ERA 5 data downloaded!")
    

    # Initialize wth_escenaries
    wth_escenaries = Resam.copy()
    wth_escenaries['chirp_data'] = wth_escenaries['coord'].apply(lambda x: extract_chirp_data(path_chirp, x))
    wth_escenaries['nasa_data'] = wth_escenaries.apply(lambda row: download_nasa_safety(start_date, end_date, row['stations'], row['coord']), axis=1)
    wth_escenaries['climatology_mean'] = wth_escenaries['stations'].apply(lambda x: clim_extract(x, start_date))

    # Perform join_wth_escenarios using parallel processing
    wth_escenaries['wth_final'] = wth_escenaries.apply(lambda row: join_wth_escenarios(row['Escenaries'], row['chirp_data'], row['nasa_data'], row['climatology_mean']), axis=1)



    # Save wth_escenaries as pickle file
    with open(f"/forecast/workdir/{country}_wth_scenaries.pkl", "wb") as f:
        pickle.dump(wth_escenaries, f)

    write_scenarios()
        
    # Remove chirp files
    #files = glob.glob(os.path.join(path_output, "*.tif"))
    #for file in files:
    #    os.remove(file)

if __name__ == "__main__":
    print("Setting global parameters")
    country = "ET"
    path = "D:\\CIAT\\Code\\USAID\\aclimate_resampling\\data\\"
    cores = 2
    force = False
    region = [14,32,3,48]
    run(path,country, region,cores, force)