# Functions to complete month in the climate scenarios
# Created by: Steven Sotelo
# Alliance Bioversity, CIAT. 2023

import sys
import os
import glob
import datetime
import urllib.request
from datetime import timedelta
from zipfile import ZipFile
import gzip
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from tqdm import tqdm
import numpy as np
import rasterio
import xarray
import multiprocessing as mp

import cdsapi # https://cds.climate.copernicus.eu/cdsapp#!/dataset/sis-agrometeorological-indicators?tab=form

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from tools import DownloadProgressBar,DirectoryManager

class CompleteData():

    # start_date: start date to download.
    def __init__(self,start_date,country,path,cores = 1,force = False):
        self.start_date = start_date
        self.country = country
        self.path = path
        self.cores = cores
        self.force = force
        self.end_date = (self.start_date + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
        self.manager = DirectoryManager()
        self.path_country = ""
        self.path_country_inputs = ""
        self.path_country_inputs_forecast = ""
        self.path_country_inputs_forecast_dailydata = ""
        self.path_country_inputs_forecast_dailydownloaded = ""
        self.path_country_outputs = ""
        self.path_country_outputs_resampling = ""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to prepare and validate the enviroment
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def prepare_env(self):
        # Creating paths
        self.path_country = os.path.join(self.path,self.country)

        self.path_country_inputs = os.path.join(self.path_country,"inputs")
        self.path_country_inputs_forecast = os.path.join(self.path_country_inputs,"prediccionClimatica")
        self.path_country_inputs_forecast_dailydata = os.path.join(self.path_country_inputs_forecast,"dailyData")
        self.path_country_inputs_forecast_dailydownloaded = os.path.join(self.path_country_inputs_forecast,"daily_downloaded")

        self.path_country_outputs = os.path.join(self.path_country,"outputs")
        self.path_country_outputs_resampling = os.path.join(self.path_country_outputs,"resampling")

        print("Validating folders needed for",self.country,"in",self.path)
        folders = [self.path_country,self.path_country_inputs,self.path_country_inputs_forecast,
                   self.path_country_inputs_forecast_dailydata,self.path_country_outputs,self.path_country_outputs_resampling]
        missing_files = ""
        missing_count = 0

        for idx, folder in enumerate(folders):
            if not os.path.exists(folder):
                missing_files = str(idx) + "-" + folder + ","
                missing_count += 1

        if missing_count > 0:
            raise ValueError("ERROR Directories don't exist (" + str(missing_count) + "): " + missing_files)

        self.manager.mkdir(self.path_country_inputs_forecast_dailydownloaded)
        print("Init:",self.start_date,"End:",self.end_date,"Year:",self.start_date.year,"Month:",self.start_date.month)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to list all weather stations
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # OUTPUT: Dataframe with a list of all weather stations
    def list_ws(self):
        errors = 0
        df_ws = pd.DataFrame(columns=["ws","lat","lon","message"])
        df_ws["ws"] =[w.split(os.path.sep)[-1] for w in glob.glob(os.path.join(self.path_country_outputs_resampling, '*'))]
        for index,row in df_ws.iterrows():
            if os.path.exists(os.path.join(self.path_country_inputs_forecast_dailydata,row["ws"] + "_coords.csv")):
                df_tmp = pd.read_csv(os.path.join(self.path_country_inputs_forecast_dailydata,row["ws"] + "_coords.csv"))
                df_ws.at[index,"lat"],df_ws.at[index,"lon"],df_ws.at[index,"message"] = df_tmp.at[0,"lat"],df_tmp.at[0,"lon"],""
            else:
                errors += 1
                df_ws.at[index,"message"] = "ERROR with coordinates"
        if errors > 0:
            print("WARNING: Stations with problems",df_ws.loc[df_ws["message"].isna() == False,:])
        df_ws["ws"] = df_ws["ws"].astype('string')
        df_ws["lat"] = df_ws["lat"].astype('float64')
        df_ws["lon"] = df_ws["lon"].astype('float64')
        df_ws["message"] = df_ws["message"].astype('string')
        return df_ws

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to download data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # url: url of the file
    # path: path to save file.
    # force: If you want to force to execute the process
    # remove: Set if you want to remove the gz file
    def download_file(self, url, path, force = False, remove = True):
        if force or os.path.exists(path.replace('.gz','')) == False:
            if os.path.exists(path.replace('.gz','')):
                os.remove(path.replace('.gz',''))
            with DownloadProgressBar(unit='B', unit_scale=True,miniters=1, desc=url.split('/')[-1]) as t:
                urllib.request.urlretrieve(url, filename=path, reporthook=t.update_to)
            with gzip.open(path, 'rb') as f_in:
                with open(path.replace('.gz',''), 'wb') as f_out:
                # Read the compressed content and write it to the output file
                    f_out.write(f_in.read())
            os.remove(path)
        else:
            print("\tFile already downloaded!",path)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to download chirp data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # test: Set if it is a test or not. If it is test, it just will download three or two files. By default it is False
    # OUTPUT: save rasters layers.
    def download_data_chirp(self, test = False):
        save_path = self.path_country_inputs_forecast_dailydownloaded
        print(save_path)
        # Create folder for data
        save_path_chirp = os.path.join(save_path,"chirp")
        self.manager.mkdir(save_path_chirp)

        # Calculate dates to download data
        if test:
            dates = [self.start_date + timedelta(days=x) for x in [0,1]]
        else:
            dates = [self.start_date + timedelta(days=x) for x in range((self.end_date - self.start_date).days + 1)]

        # Creating a list of all files that should be downloaded
        urls = [f"http://data.chc.ucsb.edu/products/CHIRP/daily/{self.start_date.year}/chirp.{date.strftime('%Y.%m.%d')}.tif.gz" for date in dates]
        files = [os.path.basename(url) for url in urls]
        save_path_chirp_all = [os.path.join(save_path_chirp, file) for file in files]
        force_all = [self.force] * len(files)

        # Download in parallel
        with ThreadPoolExecutor(max_workers=self.cores) as executor:
            executor.map(self.download_file, urls, save_path_chirp_all,force_all)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to download ERA 5 data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # variables: List of variables to download. by default all are selected
    # test: Set if it is a test or not. If it is test, it just will download two files. By default it is False
    # OUTPUT: save rasters layers.
    def download_era5_data(self,variables=["t_max","t_min","sol_rad"], test = False):
        new_crs = '+proj=longlat +datum=WGS84 +no_defs'
        # Define the variables classes and their parameters for the CDSAPI
        enum_variables ={
                            "t_max":{"name":"2m_temperature",
                                    "statistics":['24_hour_maximum'],
                                    "transform":"-",
                                    "value":273.15},
                            "t_min":{"name":"2m_temperature",
                                    "statistics":['24_hour_minimum'],
                                    "transform":"-",
                                    "value":273.15},
                            "sol_rad":{"name":"solar_radiation_flux",
                                    "statistics":[],
                                    "transform":"/",
                                    "value":1000000}
                        }

        # Create folder for data
        save_path = self.path_country_inputs_forecast_dailydownloaded
        save_path_era5 = os.path.join(save_path,"era5")
        self.manager.mkdir(save_path_era5)

        # Calculate dates to download data
        year = self.start_date.strftime("%Y")
        month = self.start_date.strftime("%m")
        if test:
            days = [(self.start_date + timedelta(days=x)).strftime("%d") for x in [0,1]]
        else:
            days = [(self.start_date + timedelta(days=x)).strftime("%d") for x in range((self.end_date - self.start_date).days + 1)]
        

        # Process for each variable that should be downloaded
        for v in variables:
            print("\tProcesing",v)
            # Creating folder for each variable
            save_path_era5 = os.path.join(save_path,"era5",v + ".zip")
            save_path_era5_data = os.path.join(save_path,"era5",v)
            save_path_era5_data_tmp = os.path.join(save_path,"era5",v + "_tmp")
            self.manager.mkdir(save_path_era5_data)
            self.manager.mkdir(save_path_era5_data_tmp)

            if self.force or os.path.exists(save_path_era5) == False:
                c = cdsapi.Client()
                c.retrieve('sis-agrometeorological-indicators',
                    {
                        'format': 'zip',
                        'variable': enum_variables[v]["name"],
                        'statistic': enum_variables[v]["statistics"],
                        'year': year,
                        'month': month,
                        'day': days,
                    },
                    save_path_era5
                )
            else:
                print("\tFile already downloaded!",save_path_era5)

            if self.force or len(os.listdir(save_path_era5_data_tmp)) == 0:
                print("\tExtracting temporally",save_path_era5)
                # loading the zip and creating a zip object
                with ZipFile(save_path_era5, 'r') as zObject:
                    # Extracting all the members of the zip
                    # into a specific location.
                    zObject.extractall(path=save_path_era5_data_tmp)
                print("\tExtracted!")
            else:
                print("\tFiles already extracted!",save_path_era5_data_tmp)

            if self.force or len(os.listdir(save_path_era5_data)) == 0:
                tmp_files = glob.glob(os.path.join(save_path_era5_data_tmp, '*'))
                print("\tSetting CRS",save_path_era5_data_tmp,len(tmp_files))
                for file in tqdm(tmp_files,desc="nc to raster and setting new CRS " + v):
                    input_file = file
                    output_file = os.path.join(save_path_era5_data,file.split(os.path.sep)[-1].replace(".nc",".tif"))

                    xds = xarray.open_dataset(input_file)
                    if enum_variables[v]["transform"] == "-":
                        xds = xds - enum_variables[v]["value"]
                    elif enum_variables[v]["transform"] == "/":
                        xds = xds / enum_variables[v]["value"]
                    xds.rio.write_crs(new_crs, inplace=True)
                    variable_names = list(xds.variables)
                    xds[variable_names[3]].rio.to_raster(output_file)
                print("\tSetted!")
            else:
                print("\tFiles already transformed!",save_path_era5_data)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to extract data from rasters
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # dir_path: path where it should take rasters files.
    # var: The name of the variable
    # locations: Dataframe with the stations
    # date_start: Position into the filename where the date starts
    # date_end: Position into the filename where the date ends
    # date_format: Format in which we can find the date in the filename
    # OUTPUT: list with values extracted by variable, date, and station.
    def extract_values(self,dir_path,var,locations, date_start,date_end,date_format):
        files = [f for f in os.listdir(dir_path) if f.endswith('.tif')]
        data = []

        # Loop for each daily file
        for file in tqdm(files,desc="Extracting " + var):
            file_path = os.path.join(dir_path, file)
            with rasterio.open(file_path) as src:
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
        #print(data)
        return data

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to extract Chirp data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # locations: Dataframe with coordinates for each location that we want to extract.
    # OUTPUT: This return resampling scenaries join with satellite data.
    def extract_chirp_data(self,locations):
        save_path = self.path_country_inputs_forecast_dailydownloaded
        dir_path = os.path.join(save_path,"chirp")
        data = self.extract_values(dir_path,'prec',locations,-14,-4,'%Y.%m.%d')
        df = pd.DataFrame(data)
        return df

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to extract ERA 5 data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # locations: Dataframe with coordinates for each location that we want to extract.
    # variables: List of variables to be extracted. by default
    # OUTPUT: This return resampling scenaries join with satellite data.
    def extract_era5_data(self,locations,variables=["t_max","t_min","sol_rad"]):
        save_path = self.path_country_inputs_forecast_dailydownloaded
        df = pd.DataFrame()
        for v in variables:
            dir_path = os.path.join(save_path,"era5",v)
            data = self.extract_values(dir_path,v,locations,-23,-15,'%Y%m%d')
            df_tmp = pd.DataFrame(data)
            if df.shape[0] == 0:
                df = df_tmp.copy()
            else:
                df = pd.merge(df,df_tmp,how='left',on=['ws','day','month','year'])
        return df

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to generate climatology from historical data
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # locations: Dataframe with coordinates for each location that we want to extract.
    # OUTPUT: This return climatology
    def extract_climatology(self,locations):
        save_path = self.path_country_inputs_forecast_dailydata
        df = pd.DataFrame()
        # Loop for each location
        for index,location in tqdm(locations.iterrows(),total=locations.shape[0],desc="Calculating climatology"):
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
                df = pd.merge(df,df_tmp,how='left',on=['ws','month','day'])

        df["year"] = self.start_date.year
        df = df[['ws','day', 'month', 'year', 'prec','t_max', 't_min', 'sol_rad']]

        return df

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to write the outputs
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # locations: Dataframe with coordinates for each location that we want to extract.
    # data: Dataframe with months generate
    def write_outputs(self,locations,data,climatology,variables=['prec','t_max','t_min','sol_rad']):
        save_path = self.path_country_outputs_resampling
        cols_date = ['day','month','year']
        cols_total = cols_date + variables
        for index,location in tqdm(locations.iterrows(),total=locations.shape[0],desc="Writing scenarios"):
            files = glob.glob(os.path.join(save_path,location["ws"], '*'))
            for f in files:
                # Preparing original files
                df_tmp = pd.read_csv(f)
                # Remove records old
                df_tmp = df_tmp.loc[(df_tmp["year"] != self.start_date.year) & (df_tmp["month"] != self.start_date.month),:]

                # filtering data for this location
                df_data = data.loc[data["ws"] == location["ws"],cols_total]

                # We validate if we have data or we should use the climatology
                if df_data.shape[0] == 0:
                    df_data = climatology.loc[climatology["ws"] == location["ws"],cols_total]

                #
                #df_data = df_data.append(df_tmp,ignore_index=True)
                df_data = pd.concat([df_data,df_tmp], ignore_index=True)
                df_data = df_data[cols_total]
                df_data.to_csv(f,index=False)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to execute chunk of the waether station list
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # df_chunk: Dataframe with the partial list of weather stations
    def run_chunk(self, df_chunk):
        print("Extracting CHIRP data")
        df_data_chirp = self.extract_chirp_data(df_chunk)
        print("Extracted CHIRP data")

        print("Extracting ERA 5 data")
        df_data_era5 = self.extract_era5_data(df_chunk)
        print("Extracted ERA 5 data")

        print("Merging CHIRPS and ERA 5")
        df_data = pd.DataFrame()
        df_data = pd.merge(df_data_chirp,df_data_era5,how='outer',on=['ws','day','month','year'])
        print("Merged CHIRPS and ERA 5")

        print("Extracting Climatology data")
        df_data_climatology = self.extract_climatology(df_chunk)
        print("Extracted Climatology data")

        print("Writing scenarios")
        self.write_outputs(df_chunk,df_data,df_data_climatology)
        print("Finished")

        return True

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Function to runs all process
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def run(self):
        print("Preparing enviroment")
        self.prepare_env()
        print("Env prepared")

        print("CHIRPS data started!")
        self.download_data_chirp()
        print("CHIRPS data downloaded!")

        print("ERA 5 data started!")
        self.download_era5_data()
        print("ERA 5 data downloaded!")

        print("Listing stations")
        df_ws = self.list_ws()
        print("Listed stations")

        print("Adding data started!")
        pool = mp.Pool(processes=self.cores)
        chunks = np.array_split(df_ws, self.cores)
        chunk_processes = [pool.apply_async(self.run_chunk, args=(chunk)) for chunk in chunks]
        print("Added data!")

        print("Process finished")
