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
- You need the .cdsapirc file which should be in $HOME if you are using linux or User Home if you use Windows

## Configure DEV Enviroment

You should create a env to run the code and install the requeriments

````bash
pip install virtualenv
venv env
pip install -r requirements.txt
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
python resampling.py "ETHIOPIA" "D:\\aclimate_resampling\\data\\" "-1" 2
````