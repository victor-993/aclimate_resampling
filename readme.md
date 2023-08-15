# AClimate resampling module

![GitHub release (latest by date)](https://img.shields.io/github/v/release/CIAT-DAPA/aclimate_resampling) ![](https://img.shields.io/github/v/tag/CIAT-DAPA/aclimate_resampling)

This repository contains all related to resampling module for AClimate

## Features

- Generate climate scenarios base on probabilities
- Complete daily data using CHIRPS and ERA 5
- Include data for case use in **data** folder
- Include modules to configure the env in **modules** folder

## Prerequisites

- Python 3.x
- GDAL
- You need the .cdsapirc file which should be in $HOME if you are using linux or User Home if you use Windows

## Configure DEV Enviroment

You should create a env to run the code and install the requeriments. 

### Linux

Run the following commands in the prompt

````bash
sudo add-apt-repository -y ppa:ubuntugis/ppa
sudo apt-get update -q
sudo apt-get install -y libgdal-dev

pip install virtualenv
venv env
pip install -r requirements.txt

pip install GDAL==$(gdal-config --version) --global-option=build_ext --global-option="-I/usr/include/gdal"
````

### Windows

You have to edit the **requirements_windows.txt** file. Change the path of the wheels for 
**GDAL** and **rasterio** package for the path of the files in **modules**. It should be absolute path.

````bash
pip install virtualenv
venv env
pip install -r requirements_windows.txt
````

## Run Test

You can run the unit testing using the following command

````bash
python -m unittest discover -s .\test
````

## Install

This module can be used as a library in other Python projects. To install this orm as a 
library you need to execute the following command:

````bash
pip install git+https://github.com/CIAT-DAPA/aclimate_resampling
````

If you want to download a specific version of orm you can do so by indicating the version tag (@v0.0.0) at the end of the install command 

````bash
pip install git+https://github.com/CIAT-DAPA/aclimate_resampling@v0.2.0
````

## Run

This module can be executed as a program:

````bash
python aclimate_resampling.py "ETHIOPIA" "D:\\aclimate_resampling\\data\\" "-1" 2 2023
````

### Params
- 0: Country  - Name of the country to be processed
- 1: Path root - Root path where the forecast is running
- 2: Previous months - Amount of months that you want to add
- 3: Cores - Number of cores to use in the calculation
- 4: Year - Year Forecast
