# https://cds.climate.copernicus.eu/cdsapp#!/dataset/sis-agrometeorological-indicators?tab=form

import os
import glob
import datetime

import datetime
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor


import pandas as pd
import numpy as np

import urllib.request
from tqdm import tqdm

import rasterio
from rasterio.transform import from_origin

import pickle

import cdsapi
import xarray as xr
import pandas as pd

from zipfile import ZipFile

class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

class DownloadData():

    # coords: North, West, South, East
    def __init__(self,start_date,country,path,region,cores = 1,force = False):
        self.start_date = start_date
        self.country = country
        self.path = path
        self.region = region
        self.cores = cores
        self.force = force
        
        pass
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function which creates a folder. It checks if the folders exist before
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # (string) path: Path where the folder should be create
    def mkdir(self,path):
        if not os.path.exists(path): 
            os.mkdir(path)

    def download_file(self, url, path, force = False):
        if force or os.path.exists(path) == False:
            with DownloadProgressBar(unit='B', unit_scale=True,miniters=1, desc=url.split('/')[-1]) as t:
                urllib.request.urlretrieve(url, filename=path, reporthook=t.update_to)
        else:
            print("\tFile already downloaded!",path)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to download chirps data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # save_path: path to save raster files.
    # start_date: start date to download.
    # end_date: end date to download.
    # year_to: resampling year.
    # cores: # cores to use in parallel.
    # force: If you want to force to execute the process
    # OUTPUT: save rasters layers.
    def download_data_chirp(self,save_path, start_date, end_date, year_to):
        # Create folder for data
        save_path_chirp = os.path.join(save_path,"chirps")
        self.mkdir(save_path_chirp)
        
        # Calculate dates to download data
        dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
        
        # Creating a list of all files that should be downloaded
        urls = [f"http://data.chc.ucsb.edu/products/CHIRP/daily/{year_to}/chirp.{date.strftime('%Y.%m.%d')}.tif" for date in dates]
        files = [os.path.basename(url) for url in urls]
        save_path_chirp_all = [os.path.join(save_path_chirp, file) for file in files]
        force_all = [self.force] * len(files)

        # Download in parallel
        with ThreadPoolExecutor(max_workers=self.cores) as executor:
            executor.map(self.download_file, urls, save_path_chirp_all,force_all)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to download ERA 5 data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # save_path: path to save raster files.
    # start_date: start date to download.
    # end_date: end date to download.
    # variables: List of variables to download. by default
    # force: If you want to force to execute the process
    # OUTPUT: save rasters layers.
    def download_era5_data(self,save_path, start_date, end_date, variables=["t_max","t_min","sol_rad"]):
        # Define the variables classes and their parameters for the CDSAPI
        enum_variables ={
                            "t_max":{"name":"2m_temperature",
                                    "statistics":['24_hour_maximum']},
                            "t_min":{"name":"2m_temperature",
                                    "statistics":['24_hour_minimum']},
                            "sol_rad":{"name":"solar_radiation_flux",
                                    "statistics":[]}
                        }
        
        # Create folder for data
        save_path_era5 = os.path.join(save_path,"era5")
        self.mkdir(save_path_era5)

        # Calculate dates to download data
        year = start_date.strftime("%Y")
        month = start_date.strftime("%m")
        days = [(start_date + timedelta(days=x)).strftime("%d") for x in range((end_date - start_date).days + 1)]

        # Process for each variable that should be downloaded
        for v in variables:
            print("\tProcesing",v)
            # Creating folder for each variable
            save_path_era5 = os.path.join(save_path,"era5",v + ".zip")
            save_path_era5_data = os.path.join(save_path,"era5",v)
            self.mkdir(save_path_era5_data)

            if self.force or os.path.exists(save_path_era5) == False:
                c = cdsapi.Client()
                c.retrieve('sis-agrometeorological-indicators',
                    {
                        'format': 'zip',
                        'variable': enum_variables[v]["name"],
                        'statistic': enum_variables[v]["statistics"],
                        'area': f'{self.coords[0]}/{self.coords[1]}/{self.coords[2]}/{self.coords[3]}',
                        'year': year,
                        'month': month,
                        'day': days,
                    },
                    save_path_era5
                )
            else:
                print("\tFile already downloaded!",save_path_era5)

            print("\tExtracting",save_path_era5)
            # loading the zip and creating a zip object
            with ZipFile(save_path_era5, 'r') as zObject:
                # Extracting all the members of the zip 
                # into a specific location.
                zObject.extractall(path=save_path_era5_data)
            print("\tExtracted!")




    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to extract Chirp data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # save_path:  rasters path
    # locations: list of coordinates for each location that we want to extract.
    # OUTPUT: This return resampling scenaries join with satellite data.
    def extract_chirp_data(self,save_path, locations):
        files = [f for f in os.listdir(save_path) if f.endswith('.tif')]
        data = []
        
        for file in files:
            file_path = os.path.join(save_path, file)
            with rasterio.open(file_path) as dataset:
                for location in locations:
                    row, col = dataset.index(location['lon'], location['lat'])
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
    def clim_extract(self,wth_data, ini_date):
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

    def join_wth_escenarios(self,Escenaries, chirp_data, nasa_data, climatology_mean):
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


    def run(self,start_date):
        ## up 2021, jre: Se estandariza el uso de formato fecha, se requiere paquete lubridate
        print("Calculating dates for the process")
        end_date = (self.start_date + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
        year_to = self.start_date.year
        month_to = self.start_date.month
        print("Init:",self.start_date,"End:",end_date,"Year:",year_to,"Month:",month_to)

        # Validating folder
        print("Validating folders")
        path_country = os.path.join(self.path,self.country)
        path_inputs = os.path.join(path_country,"inputs")
        path_outputs = os.path.join(path_country,"outputs")
        path_daily = os.path.join(path_inputs,"prediccionClimatica","dailyData")
        path_resampling = os.path.join(path_outputs,"prediccionClimatica","resampling")
        path_daily_downloaded = os.path.join(path_inputs,"prediccionClimatica","daily_downloaded") # It is not included because we will create after

        folders = [path_country,path_inputs,path_outputs,path_daily,path_resampling]
        missing_files = []

        for folder in folders:
            if not os.path.exists(folder):
                missing_files.append(folder)

        if len(missing_files) > 0:
            print("Directories don't exist",missing_files)
        else:
            self.mkdir(path_daily_downloaded)

            # Download Chirps data
            print("CHIRPS data started!")
            self.download_data_chirp(path_daily_downloaded, start_date, end_date, year_to)
            print("CHIRPS data downloaded!")

            # Download ERA 5 data
            print("ERA 5 data started!")
            self.download_era5_data(path_daily_downloaded, start_date, end_date)
            print("ERA 5 data downloaded!")
            

            # Initialize wth_escenaries
            #wth_escenaries = Resam.copy()
            #wth_escenaries['chirp_data'] = wth_escenaries['coord'].apply(lambda x: self.extract_chirp_data(path_chirp, x))
            #wth_escenaries['nasa_data'] = wth_escenaries.apply(lambda row: download_nasa_safety(start_date, end_date, row['stations'], row['coord']), axis=1)
            #wth_escenaries['climatology_mean'] = wth_escenaries['stations'].apply(lambda x: clim_extract(x, start_date))

            # Perform join_wth_escenarios using parallel processing
            #wth_escenaries['wth_final'] = wth_escenaries.apply(lambda row: join_wth_escenarios(row['Escenaries'], row['chirp_data'], row['nasa_data'], row['climatology_mean']), axis=1)



            # Save wth_escenaries as pickle file
            #with open(f"/forecast/workdir/{country}_wth_scenaries.pkl", "wb") as f:
            #    pickle.dump(wth_escenaries, f)

            #write_scenarios()
                
            # Remove chirp files
            #files = glob.glob(os.path.join(path_output, "*.tif"))
            #for file in files:
            #    os.remove(file)

