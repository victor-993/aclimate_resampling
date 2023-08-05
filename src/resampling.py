  # -*- coding: utf-8 -*-
  # Functions to do climate daily data forecast per station
  # Created by: Maria Victoria Diaz
  # Alliance Bioversity, CIAT. 2023

import pandas as pd
import calendar
import numpy as np
import random
import os
import warnings

warnings.filterwarnings("ignore")

class AClimateResampling():

  def __init__(self,path,country,cores, year_forecast):
     self.path = path
     self.country = country
     self.cores = cores
     self.path_inputs = os.path.join(self.path,self.country,"inputs")
     self.path_inputs_prediccion = os.path.join(self.path_inputs,"prediccionClimatica")
     self.path_inputs_daily = os.path.join(self.path_inputs_prediccion,"dailyData")
     self.path_outputs = os.path.join(self.path,self.country,"outputs")
     #self.path_outputs_prediccion = os.path.join(self.path_outputs,"prediccionClimatica")
     self.path_outputs_prob = os.path.join(self.path_outputs,"probForecast")
     self.year_forecast = year_forecast
     self.npartitions = int(round(cores/3)) 

     pass

  def mdl_verification(self,daily_weather_data, seasonal_probabilities):


      clima = os.listdir(daily_weather_data)
      clima = [file for file in clima if not file.endswith("_coords.csv")]
      clima = [file.split(".csv")[0] for file in clima]

      prob = pd.read_csv(seasonal_probabilities)



      prob = prob[prob['id'].isin(clima)]



      check_clm = []
      for i in range(len(clima)):
          df = pd.read_csv(os.path.join(daily_weather_data, f"{clima[i]}.csv"))

          # 1. max de temp_max == min de temp_max
          # 2. max de temp_min == min de temp_min
          # 3. max de srad == min de srad

          max_tmax = df['t_max'].max()
          min_tmax = df['t_max'].min()

          max_tmin = df['t_min'].max()
          min_tmin = df['t_min'].min()

          max_srad = df['sol_rad'].max()
          min_srad = df['sol_rad'].min()

          if max_tmax == min_tmax or max_tmin == min_tmin or max_srad == min_srad:
              resultado = pd.DataFrame({'code': [clima[i]], 'value': [f"tmax = {max_tmax}; tmin = {max_tmin}; srad = {max_srad}"]})
          else:
              resultado = pd.DataFrame({'code': [clima[i]], 'value': ["OK"]})
          check_clm.append(resultado)

      df = pd.concat(check_clm)
      df_1 = df[df['value'] == "OK"]
      df_2 = df[df['value'] != "OK"]

      code_ok = df_1
      code_problema = df_2


      # 1. Probabilidades con cero categoria normal
      # 2. Probabilidades sumen > 1.1
      # 3. Probabilidades sumen <  0.9

      prob['sum'] = prob['below'] + prob['normal'] + prob['above']
      prob.loc[prob['normal'] == 0.00, 'normal'] = -1
      prob.loc[prob['sum'] < 0.9, 'normal'] = -1
      prob.loc[prob['sum'] > 1.1, 'normal'] = -1

      df_1 = prob[prob['normal'] == -1]
      df_2 = prob[(prob['normal'] >= 0) & (prob['normal'] <= 1)]

      code_p_ok = df_2
      code_p_malos = df_1

      ids_buenos = code_p_ok['id'].tolist()

      result_clima_prob_outside = list(set(clima) - set(code_p_ok['id']))

      code_problema = pd.DataFrame({'ids': [1] + code_problema['code'].tolist(),
                                    'descripcion': [None] + code_problema['value'].tolist()})
      code_p_malos = pd.DataFrame({'ids': [1] + code_p_malos['id'].tolist(),
                                  'descripcion': "Problemas base de datos probabilidad"})

      result_clima_prob_outside = pd.DataFrame({'ids': [1] + result_clima_prob_outside,
                                                'descripcion': "La estacion esta fuera de area predictora"})

      ids_malos = pd.concat([code_problema, code_p_malos, result_clima_prob_outside])
      ids_buenos = pd.DataFrame({'ids': ids_buenos})
      ids_malos = ids_malos.replace(1, pd.NA).dropna()

      result = {'ids_buenos': ids_buenos, 'ids_malos': ids_malos}
      return result

  def preprocessing(self,prob_root,  ids):

    """ Determine seasons of analysis according to the month of forecast in CPT
    
    Args:

    prob_root: str
              The root of the probabilities file from CPT, with its name and extension.


    ids: dict
              Dictionary with a list of stations with problems and not to be analyzed, and 
              a list of stations without problems.

    Returns:

      Dataframe
          a dataframe with the original columns + name of season + start month of season + 
          end month of season

    """


    # Get the name of months 
    months_names = list(calendar.month_name)[1:]

    # Read the CPT probabilities file 

    proba = pd.read_csv(prob_root)
    ids_x = ids[0]['ids_buenos']
    prob = proba[proba['id'].isin(ids_x['ids'])]
    forecast_period = ids[1]

    # Check the period of forecast
    if forecast_period == "tri":

      # Generate a string of the first letters of month names
      months_names =months_names +['January', 'February']
      months_names = [word[:3] for word in months_names]
     
      # Create a list of month numbers from 1 to 12, followed by [1, 2] to create quarters
      months_numbers =list(range(1,13)) + [1,2]

      # Create a DataFrame representing periods of three consecutive months (with its numbers)
      period_numbers = pd.DataFrame( [months_numbers[i:i+3] for i in range(0, len(months_numbers)-2)])

      # Create a DataFrame representing periods of three consecutive month initials
      combination = pd.DataFrame(['-'.join(months_names[i:i+3]) for i in range(len(months_names)-2)])

      # Combine quarter's names with its month numbers in a DataFrame and change columns names
      period = pd.concat([combination, period_numbers], axis = 1)
      period.columns = ['Season','Start', 'Central_month', 'End']

      # Merge the prob DataFrame with the period DataFrame based on the 'month' and 'Central_month' columns
      prob = prob.merge(period, left_on='month', right_on='Central_month')
      prob.drop(['month','Central_month'], axis = 1, inplace = True )

    else:
      if forecast_period == "bi":

        # Generate a string of the first letters of month names
        months_names = [word[:3] for word in months_names]

        # Create a list of month numbers from 1 to 12
        months_numbers = list(range(1,13))
        
        # Create a DataFrame representing periods of two consecutive months (with its numbers)
        period_numbers = pd.DataFrame( [months_numbers[i:i+2] for i in range(0, len(months_numbers),2)])
        
        # Create a DataFrame representing periods of two consecutive month initials
        combination = pd.DataFrame(['-'.join(months_names[i:i+2]) for i in range(0, len(months_names),2)])

        # Combine bimonths with its month numbers in a DataFrame and change columns names
        period = pd.concat([combination, period_numbers], axis = 1)
        period.columns = ['Season','Start', 'End']
        
        # Merge the prob DataFrame with the period DataFrame based on the 'month' and 'Start' month columns
        prob_a = prob.merge(period, left_on='month', right_on='Start')

        # Merge the prob DataFrame with the period DataFrame based on the 'month' and 'End' month columns
        # Join with prob_a
        prob = prob_a.append(prob.merge(period, left_on='month', right_on='End'))
        prob.drop(['month'], axis = 1, inplace = True )

    # Reshape the 'prob' DataFrame and put the 'below', 'normal' and 'above' probability categories in a column
    prob = prob.melt(id_vars = ['year', 'id', 'Season', 'Start','End'], var_name = 'Type', value_name = 'Prob')
    
    #Return probability DataFrame
    return prob

  def forecast_station(self,station, prob, daily_data_root, output_root, year_forecast, forecast_period):
    
    """ Generate  forecast scenaries
    
    Args:

      station: str
            The id of th station
    
      prob: DataFrame
              The result of preprocessing function
    
      daily_data_root: str
              Where the climate data by station is located

      output_root: str
              Where outputs are going to be saved.

      year_forecast: int
              Year to forecast

      forecast_period: str
              'bi' if the period of CPT forecast is bimonthly.
              'tri' if the period of CPT forecast is quarter.

    Returns:

      Dataframe
          a dataframe with climate daily data for every season and escenary id 
          a dataframe with years of escenary for every season

    """
    # Create folders to save result

    if os.path.exists(output_root + "/"+station):
        output_estacion = output_root + "/"+station
    else:
        os.mkdir(output_root +"/"+ station)
        output_estacion = output_root +"/"+ station
    
    now = datetime.now()
    os.mkdir(output_estacion+'/'+ now.strftime("%d-%m-%Y_%H-%M-%S"))

    output_estacion = output_estacion+'/'+ now.strftime("%d-%m-%Y_%H-%M-%S")


    # Read the climate data for the station
    clim = pd.read_csv(daily_data_root + "/"+station +".csv")

    # Filter the probability data for the station
    cpt_prob = prob[prob['id']==station]

    if len(cpt_prob.index) == 0:
      print('Station does not have probabilites')
      base_years = 0
      seasons_range = 0
      p = {'id': [station],'issue': ['Station does not have probabilites']}
      problem = pd.DataFrame(p)

      return base_years, seasons_range, problem

    else:
      # Get the season for the forecast
      season = cpt_prob['Season'].iloc[0]

      # Adjust the year if the forecast period is 'tri' if necessary
      if forecast_period == 'tri':
        year_forecast = [year_forecast+1 if x in ['Dec-Jan-Feb']  else year_forecast for x in season][0]
      
      # Check if year of forecast is a leap year for February
      leap_forecast = (year_forecast%400 == 0) or (year_forecast%4==0 and year_forecast%100!=0)

      # Filter the February data for leap years
      clim_feb = clim.loc[clim['month'] == 2]
      clim_feb['leap'] = [True if (year%400 == 0) or (year%4==0 and year%100!=0) else False for year in clim_feb['year']]

      # Standardize february months by year according to year of forecat
      february = []
      for i in np.unique(clim_feb['year']):
        year_data =  clim_feb.loc[clim_feb['year']==i,:]
        year = year_data.loc[:,'leap']
        
        # If year of forecast is a leap year and a year in climate data is not, then add one day to february in climate data
        if leap_forecast == True and year.iloc[0] == False:
          year_data.append(year_data.sample(1), ignore_index=True)
          year_data.iloc[-1,0] = 29
        else:

          # If year of forecast is not a leap year and a year in climate data is, then remove one day to february in climate data
          if leap_forecast == False and year.iloc[0] == True:
            year_data =  year_data.iloc[:-1]
          else:

            # If both year of forecast and year in climate data are leap years or not, then keep climate data the same
            year_data = year_data
      february.append(year_data)

      # Concat standardized february data with the rest of climate data
      data = pd.concat(february).drop(['leap'], axis = 1 )
      data = pd.concat([data,clim.loc[clim['month'] != 2]]).sort_values(['year','month'])


      # Start the resampling process for every season of analysis in CPT probabilities file

      base_years = [] # List to store years of sample for each season
      seasons_range = [] # List to store climate data in the years of sample for each season

      for season in  list(np.unique(cpt_prob['Season'])):

        # Select the probabilities for the season
        x = cpt_prob[cpt_prob['Season'] == season] 

        if x['Start'].iloc[0] > x['End'].iloc[0]:
          # In climate data 
          # If the start month is greater than the end month of the season, select the months from the start and less than the end
            data_range = data.loc[data['month'] >= x['Start'].iloc[0]]
            data_range_2 = data.loc[data['month'] <= x['End'].iloc[0]]
            data_range = data_range.append(data_range_2)

        else:
            #In climate data
            #If not, select the months between the start and the end month of season
            data_range = data.loc[(data['month'] >= x['Start'].iloc[0]) & (data['month'] <= x['End'].iloc[0])]

      # Compute total precipitation for each year in the climate data range selected
        new_data = data_range[['year','prec']].groupby(['year']).sum().reset_index()

        merge = data_range
        merge['Season'] = season

      # Calculate quantiles to determine precipitation conditions for every year in climate data selected
        cuantiles = list(np.quantile(new_data['prec'], [.33,.66]))
        new_data['condition'] =  'NA'
        new_data.loc[new_data['prec']<= cuantiles[0], 'condition'] = 'below'
        new_data.loc[new_data['prec']>= cuantiles[1], 'condition'] =  'above'
        new_data.loc[(new_data['prec']> cuantiles[0]) & (new_data['prec']< cuantiles[1]), 'condition'] =  'normal'
        
      # Sample 100 records in probability file of season based on probability from CPT as weights
        muestras = x[['Start', 'End', 'Type', 'Prob']].sample(100, replace = True, weights=x['Prob'])
        muestras = muestras.set_index(pd.Index(list(range(0,100))))
      
      # Randomly get one year from the total precipitation data based on precipitation conditions selected in the 100 data sample.
        muestras_by_type = []
        for i in muestras.index: 
          m = new_data.loc[new_data['condition'] == muestras['Type'].iloc[i]].sample(1)
          muestras_by_type.append(m) 
        
        # Join the 100 samples and add sample id
        muestras_by_type = pd.concat(muestras_by_type).reset_index()
        muestras_by_type['index'] = muestras.index
        muestras_by_type = muestras_by_type.set_index(pd.Index(list(range(0,100))))


        # Rename year column with season name  
        muestras_by_type = muestras_by_type.rename(columns = {'year':season})

        # Calculate the next year of the year sample and assign the same sample id 
        muestras_by_type['plus'] = list(map(lambda x: x + 1, muestras_by_type[season]))

        #Set the sample years as list and sort
        years = list(muestras_by_type[season])
        years.sort()

        if season == 'Nov-Dec-Jan':
          # If season is November-December-January 

          years_plus = list(map(lambda x: x + 1, years))
          years_plus.sort()
        
          months_numbers =[11,12]

          # Filter the climate data of the last two months of the years in the sample and get the sample id 
          merge_a = merge[merge['year'].isin(years)]
          merge_a = merge_a[merge_a['month'].isin(months_numbers)]
          merge_a = pd.merge(merge_a, muestras_by_type[['index', season]], left_on = 'year', right_on = season)
          merge_a.drop(season, axis = 1,inplace = True)

          # Filter the climate data of the first month in the next year of the years in sample and get the sample id
          merge_b = merge[merge['year'].isin(years_plus)]
          merge_b = merge_b[merge_b['month'] == 1]
          merge_b = pd.merge(merge_b, muestras_by_type[['index', 'plus']], left_on = 'year', right_on = 'plus')
          merge_b.drop('plus', axis = 1,inplace = True)

          # Merge the climate data filtered
          merge = merge_a.append(merge_b)
            
        else:
          if season == 'Dec-Jan-Feb':
            # If season is December-January-February

            years_plus = list(map(lambda x: x + 1, years))
            years_plus.sort()
            months_numbers =[1,2]

            # Filter the climate data of the last month of the years in the sample and get the sample id 
            merge_a = merge[merge['year'].isin(years)]
            merge_a = merge_a[merge_a['month'] == 12]
            merge_a = pd.merge(merge_a, muestras_by_type[['index', season]], left_on = 'year', right_on = season)
      
            # Filter the climate data of the first two months in the next year of the years in sample and get the sample id
            merge_b = merge[merge['year'].isin(years_plus)]
            merge_b = merge_b[merge_b['month'].isin(months_numbers)]
            merge_b = pd.merge(merge_b, muestras_by_type[['index', 'plus']], left_on = 'year', right_on = 'plus')
            merge_b.drop('plus', axis = 1,inplace = True)

            # Merge filtered data
            merge = merge_a.append(merge_b)

          else:
            # If season is another, filter climate data of the years in sample and get the sample id

            merge = merge.loc[merge['year'].isin(muestras_by_type[season])]
            merge = pd.merge(merge,muestras_by_type[['index',season]],left_on = 'year', right_on = season)
            merge = merge.drop(season, axis = 1)


        # Append the 100 years in the sample for every season in the list
        base_years.append(muestras_by_type[['index',season]])

        # Append the climate data filtered for every season in the list
        seasons_range.append(merge)

   
      # Join seasons samples by column by sample id and save DataFrame in the folder created
      base_years = pd.concat(base_years, axis = 1).rename(columns={'index': 'id'})

      if len(list(np.unique(cpt_prob['Season']))) ==2:
            base_years = base_years.iloc[:,[0,1,3] ]
            base_years.to_csv(output_estacion+ "/samples_for_forecast_"+ forecast_period +".csv", index = False)

            # Join climate data filtered for the seasons and save DataFrame in the folder created
            seasons_range = pd.concat(seasons_range).rename(columns={'index': 'id'})

            #Return climate data filtered with sample id 
            return base_years, seasons_range

      else:
            print('Station just have one season available')
            base_years = base_years.iloc[:,[0,1] ]
            p = {'id': [station],'issue': ['Station just have one season available'], 'season': [base_years.columns[1]]}
            problem = pd.DataFrame(p)
            base_years.to_csv(output_estacion+ "/samples_for_forecast_"+ forecast_period +".csv", index = False)

            # Join climate data filtered for the seasons and save DataFrame in the folder created
            seasons_range = pd.concat(seasons_range).rename(columns={'index': 'id'})

            #Return climate data filtered with sample id 
            return base_years, seasons_range, problem

  def save_forecast(self,output_estacion, year_forecast, prob, seasons_range, base_years, station):


    """ Save the climate daily data by escenary and a summary of the escenary
    
    Args:

      output_root: str
              Where outputs are going to be saved.
      year_forecast: int
              Year to forecast

      forecast_period: str
              'bi' if the period of CPT forecast is bimonthly.
              'tri' if the period of CPT forecast is quarter.

      prob: DataFrame
              The result of preprocessing function

      seasons_range: DataFrame
              The result of forecast_station function

      base_years: DataFrame 
              The result of forecast_station function

      station: str
            The id of th station    

    
    Returns:
          None
    """
    if isinstance(base_years, pd.DataFrame):
    # Set the output root based on forecast period


      # Filter probability DataFrame by station
      cpt_prob = prob[prob['id']==station]

      # If forecast period is November-December-January or December-January-February then the year of forecast is the next
      year_forecast = [year_forecast+1 if x in ['NDJ', 'DJF']  else year_forecast for x in cpt_prob['Season'].iloc[0]][0]

      # Filter climate data by escenry id and save
      escenarios = []
      for i in base_years.index:
          df = seasons_range[(seasons_range['id'] == base_years['id'].iloc[i])]
          df['year'] = year_forecast
          df = df.drop(['id', 'Season'], axis = 1)
          escenarios.append(df)
          df.to_csv(output_estacion +"/escenario_"+ str(i)+".csv", index=False)

      print("Escenaries saved in {}".format(output_estacion))

      if os.path.exists(output_estacion+ "/summary/"):
          summary_path = output_estacion+ "summary/"
      else:
          os.mkdir(output_estacion+ "/summary/")
          summary_path = output_estacion+ "/summary/"

      # Calculate maximum and minimum of escenaries by date and save
      df = pd.concat(escenarios)
      df.groupby(['day', 'month']).max().reset_index().sort_values(['month', 'day'], ascending = True).to_csv(summary_path+ station+"_max.csv", index=False)
      df.groupby(['day', 'month']).min().reset_index().sort_values(['month', 'day'], ascending = True).to_csv(summary_path+ station+"_min.csv", index=False)
      print("Minimum and Maximum of escenaries saved in {}".format(summary_path))

    else:

      return None
    

  def master_processing(self,station, input_root, climate_data_root, verifica ,output_root, year_forecast):

    if os.path.exists(output_root):
        output_root = output_root
    else:
        os.mkdir(output_root)
        


    print("Reading the probability file and getting the forecast seasons")
    prob_normalized = self.preprocessing(input_root, verifica, output_root)


    print("Resampling and creating the forecast scenaries")
    resampling_forecast = self.forecast_station(station = station,
                                           prob = prob_normalized,
                                           daily_data_root = climate_data_root,
                                           output_root = output_root,
                                           year_forecast = year_forecast,
                                           forecast_period = verifica[1])


    print("Saving escenaries and a summary")
    self.save_forecast(output_estacion = resampling_forecast[2],
                  year_forecast = year_forecast,
                  prob = prob_normalized,
                  base_years = resampling_forecast[0],
                  seasons_range = resampling_forecast[1],
                  station = station)

    if len(resampling_forecast) == 4:
        oth = output_root+ "/issues.csv"
        resampling_forecast[3].to_csv(oth, mode='a', index=False, header=not os.path.exists(oth))

    else:
        return None
    

  def resampling(self):


    
    print("Fixing issues in the databases")
    verifica = self.mdl_verification(self.path_inputs_daily, self.path_outputs_prob)
    
    
    estaciones = os.listdir(self.path_inputs_daily)
    n = [i for i in estaciones if not  i.endswith("_coords.csv") ]
    n = [i.replace(".csv","") for i in n]
    n1 = [i for i in n if i in list(verifica[0]['ids_buenos']['ids'])]

  
    print("Processing resampling for stations")

    
    n_df = pd.DataFrame(n1,columns=["id"])
    n_df_dd = dd.from_pandas(n_df,npartitions=self.npartitions)
    _col = {'id': object
            ,'issue':object
            , 'season': object
            , 'name': object}
    sample = n_df_dd.map_partitions(lambda df:
                                    df["id"].apply(lambda x: self.master_processing(station = x,
                                               input_root =  self.path_outputs_prob,
                                               climate_data_root = self.path_inputs_daily,
                                               output_root = self.path_outputs,
                                               verifica = verifica,
                                               year_forecast = self.year_forecast)
                                                  ), meta=_col
                                                  ).compute(scheduler='processes')
    return sample
   
  