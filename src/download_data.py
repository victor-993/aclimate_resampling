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

    # start_date: start date to download.
    # coords: North, West, South, East
    def __init__(self,start_date,country,path,region,cores = 1,force = False):
        self.start_date = start_date
        self.country = country
        self.path = path
        self.region = region
        self.cores = cores
        self.force = force
        self.end_date = (self.start_date + pd.DateOffset(months=1)) - pd.DateOffset(days=1)

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
    # year_to: resampling year.
    # cores: # cores to use in parallel.
    # force: If you want to force to execute the process
    # OUTPUT: save rasters layers.
    def download_data_chirp(self,save_path, year_to):
        # Create folder for data
        save_path_chirp = os.path.join(save_path,"chirps")
        self.mkdir(save_path_chirp)

        # Calculate dates to download data
        dates = [self.start_date + timedelta(days=x) for x in range((self.end_date - self.start_date).days + 1)]

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
    # variables: List of variables to download. by default
    # force: If you want to force to execute the process
    # OUTPUT: save rasters layers.
    def download_era5_data(self,save_path, variables=["t_max","t_min","sol_rad"]):
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
        year = self.start_date.strftime("%Y")
        month = self.start_date.strftime("%m")
        days = [(self.start_date + timedelta(days=x)).strftime("%d") for x in range((self.end_date - self.start_date).days + 1)]

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
    # Function to extract data from rasters
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # dir_path: path where it should take rasters files.
    # var: The name of the variable
    # locations: Dataframe with the stations
    # ext: Extension files
    # date_start: Position into the filename where the date starts
    # date_end: Position into the filename where the date ends
    # date_format: Format in which we can find the date in the filename
    # OUTPUT: list with values extracted by variable, date, and station.
    def extract_values(self,dir_path,var,locations, ext, date_start,date_end,date_format):
        files = [f for f in os.listdir(dir_path) if f.endswith(ext)]
        data = []

        # Loop for each daily file
        for file in tqdm(files,desc="Extracting " + var):
            file_path = os.path.join(dir_path, file)
            with rasterio.open(file_path) as src:
                # transform = src.transform
                # Loop for each location
                for index,location in locations.iterrows():
                    #col, row = ~transform * (location['lon'], location['lat'])
                    row, col = src.index(location['lon'], location['lat'])
                    value = src.read(1)[row, col]
                    date_str = file[date_start:date_end]
                    date = datetime.datetime.strptime(date_str, date_format)
                    data.append({'ws':location['ws'],
                                'day':date.day,
                                'month':date.month,
                                'year': date.year,
                                var: value})

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to extract Chirp data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # save_path:  rasters path
    # locations: Dataframe with coordinates for each location that we want to extract.
    # OUTPUT: This return resampling scenaries join with satellite data.
    def extract_chirp_data(self,save_path, locations):
        dir_path = os.path.join(save_path,"chirps")
        data = self.extract_values(dir_path,'prec',locations,'.tif',-14,-4,'%Y.%m.%d')
        df = pd.DataFrame(data)
        return df

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to extract ERA 5 data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # save_path:  rasters path
    # locations: Dataframe with coordinates for each location that we want to extract.
    # variables: List of variables to be extracted. by default
    # OUTPUT: This return resampling scenaries join with satellite data.
    def extract_era5_data(self,save_path, locations,variables=["t_max","t_min","sol_rad"]):
        df = pd.DataFrame()
        for v in variables:
            dir_path = os.path.join(save_path,"era5",v)
            data = self.extract_values(dir_path,v,locations,'.nc',-22,-14,'%Y%m%d')
            df_tmp = pd.DataFrame(data)
            if df.shape[0] == 0:
                df = df_tmp.copy()
            else:
                df = pd.merge(df,df_tmp,how='left',on=['ws','day','month','year'])
        return df

    def extract_climatology(self,save_path,locations):
        df = pd.DataFrame()
        # Loop for each location
        for index,location in tqdm(locations.iterrows(),desc="Calculating climatology"):
            file_path = os.path.join(save_path,location["ws"] + ".csv")
            df_tmp = pd.read_csv(file_path)
            df_tmp = df_tmp.groupby(['day', 'month']).agg({
                            't_max': 'mean',
                            't_min': 'mean',
                            'prec': 'median',
                            'sol_rad': 'mean'
                    }).reset_index()
            df_tmp = df_tmp.loc[df_tmp['month'] == self.start_date.month,:]
            df_tmp['ws'] = location["ws"]

            if df.shape[0] == 0:
                df = df_tmp.copy()
            else:
                df = pd.merge(df,df_tmp,how='left',on=['ws','year','month','day'])

        df["year"] = self.start_date.year
        df = df[['ws','day', 'month', 'year', 'prec','t_max', 't_min', 'sol_rad']]

        return df

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

    def write_outputs(self,save_path,locations,data,climatology,variables=['prec','t_max','t_min','sol_rad']):
        cols_date = ['day','month','year']
        cols_total = cols_date + variables
        for index,location in tqdm(locations.iterrows(),desc="Writing scenarios"):
            files = glob.glob(os.path.join(save_path, '*'))
            for f in files:
                df_tmp = pd.read_csv(f)
                df_data = data.loc[data["ws"] == location["ws"],cols_total]
                # We have to use climatology
                if df_data.shape[0] == 0:
                    df_data = climatology.loc[climatology["ws"] == location["ws"],cols_total]
                df_data = df_data.append(df_tmp,ignore_index=True)
                df_data.write_csv(f)



    def run(self):
        ## up 2021, jre: Se estandariza el uso de formato fecha, se requiere paquete lubridate
        print("Calculating dates for the process")
        year_to = self.start_date.year
        month_to = self.start_date.month
        print("Init:",self.start_date,"End:",self.end_date,"Year:",year_to,"Month:",month_to)

        # Validating folder
        print("Validating folders")
        path_country = os.path.join(self.path,self.country)
        path_inputs = os.path.join(path_country,"inputs")
        path_outputs = os.path.join(path_country,"outputs")
        path_daily = os.path.join(path_inputs,"prediccionClimatica","dailyData")
        path_resampling = os.path.join(path_outputs,"prediccionClimatica","resampling")
        path_daily_data = os.path.join(path_inputs,"prediccionClimatica","dailyData") # It is not included because we will create after
        path_daily_downloaded = os.path.join(path_inputs,"prediccionClimatica","daily_downloaded") # It is not included because we will create after

        folders = [path_country,path_inputs,path_outputs,path_daily,path_resampling]
        missing_files = []

        for folder in folders:
            if not os.path.exists(folder):
                missing_files.append(folder)

        if len(missing_files) > 0:
            print("ERROR Directories don't exist",missing_files)
        else:
            self.mkdir(path_daily_downloaded)

            # Download Chirps data
            print("CHIRPS data started!")
            self.download_data_chirp(path_daily_downloaded, year_to)
            print("CHIRPS data downloaded!")

            # Download ERA 5 data
            print("ERA 5 data started!")
            self.download_era5_data(path_daily_downloaded)
            print("ERA 5 data downloaded!")

            # Final list of stations to be processed
            print("Listing final stations")
            errors = 0
            df_ws = pd.DataFrame(columns=["ws","lat","lon","message"])
            df_ws["ws"] =[w.split(os.path.sep)[-1] for w in glob.glob(os.path.join(path_resampling, '*'))]
            for index,row in df_ws.iterrows():
                try:
                    df_tmp = pd.read_csv(os.path.join(path_daily_data,row["ws"] + "_coords.csv"))
                    df_ws.at[index,"lat"],df_ws.at[index,"lon"] = df_tmp.at[0,"lat"],df_tmp.at[0,"lon"]
                except Exception:
                    errors += 1
                    df_ws.at[index,"message"] = "ERROR with coordinates"

            if errors > 0:
                print("WARNING: Stations with problems",df_ws.loc[df_ws["message"].isna() == False,:])

            df_data = pd.DataFrame()

            print("Extracting CHIRPS data")
            df_data_chirps = self.extract_chirp_data(path_daily_downloaded,df_ws)
            print("Extracted CHIRPS data")

            print("Extracting ERA 5 data")
            df_data_era5 = self.extract_era5_data(path_daily_downloaded,df_ws)
            print("Extracted ERA 5 data")

            print("Merging CHIRPS and ERA 5")
            df_data = pd.merge(df_data_chirps,df_data_era5,how='outer',on=['ws','day','month','year'])
            print("Merged CHIRPS and ERA 5")

            ########### Code to validate

            print("Extracting Climatology data")
            df_data_climatology = self.extract_climatology(path_daily_data,df_ws)
            print("Extracted Climatology data")

            print("Writing scenarios")
            self.write_outputs(path_resampling,df_ws,df_data,df_data_climatology)
            print("Finished")




            
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

