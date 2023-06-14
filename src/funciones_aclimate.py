# -*- coding: utf-8 -*-
# Functions to do climate daily data forecast per station
# Created by: Maria Victoria Diaz
# Alliance Bioversity, CIAT. 2023



def preprocessing(prob_root,  output_root, forecast_period):

  """ Determine seasons of analysis according to the month of forecast in CPT
  
  Args:

  prob_root: str
            The root of the probabilities file from CPT, with its name and extension.

  output_root: str
            Where outputs are going to be saved.

  forecast_period: str
            'bi' if the period of CPT forecast is bimonthly.
            'tri' if the period of CPT forecast is quarter.

  Returns:

    Dataframe
        a dataframe with the original columns + name of season + start month of season + 
        end month of season

  """


  # Get the name of months 
  months_names = list(calendar.month_name)[1:]

  # Read the CPT probabilities file 
  prob = pd.read_csv(prob_root)
  prob['below'] = prob['normal'] = prob['above'] = 1/3

  # Check the period of forecast
  if forecast_period == "tri":

    # Generate a string of the first letters of month names
    months = ''.join([x[0] for x in months_names])+'JF'

    # Create a list of month numbers from 1 to 12, followed by [1, 2] to create quarters
    months_numbers =list(range(1,13)) + [1,2]

    # Create a DataFrame representing periods of three consecutive months (with its numbers)
    period_numbers = pd.DataFrame( [months_numbers[i:i+3] for i in range(0, len(months_numbers)-2)])

    # Create a DataFrame representing periods of three consecutive month initials
    period = pd.DataFrame([months[i:i+3] for i in range(0, len(months))][:-2])

    # Combine quarter's names with its month numbers in a DataFrame and change columns names
    period = pd.concat([period, period_numbers], axis = 1)
    period.columns = ['Season','Start', 'Central_month', 'End']

    # Merge the prob DataFrame with the period DataFrame based on the 'month' and 'Central_month' columns
    prob = prob.merge(period, left_on='month', right_on='Central_month')
    prob.drop(['month','Central_month'], axis = 1, inplace = True )

  else:
    if forecast_period == "bi":

      # Generate a string of the first letters of month names
      months = ''.join([x[0] for x in months_names])

      # Create a list of month numbers from 1 to 12
      months_numbers = list(range(1,13))
      
      # Create a DataFrame representing periods of two consecutive months (with its numbers)
      period_numbers = pd.DataFrame( [months_numbers[i:i+2] for i in range(0, len(months_numbers),2)])
      
      # Create a DataFrame representing periods of two consecutive month initials
      period = pd.DataFrame([months[i:i+2] for i in range(0, len(months),2)])

      # Combine bimonths with its month numbers in a DataFrame and change columns names
      period = pd.concat([period, period_numbers], axis = 1)
      period.columns = ['Season','Start', 'End']
      
      # Merge the prob DataFrame with the period DataFrame based on the 'month' and 'Start' month columns
      prob_a = prob.merge(period, left_on='month', right_on='Start')

      # Merge the prob DataFrame with the period DataFrame based on the 'month' and 'End' month columns
      # Join with prob_a
      prob = prob_a.append(prob.merge(period, left_on='month', right_on='End'))
      prob.drop(['month'], axis = 1, inplace = True )

  # Reshape the 'prob' DataFrame and put the 'below', 'normal' and 'above' probability categories in a column
  prob = prob.melt(id_vars = ['year', 'id', 'Season', 'Start','End'], var_name = 'Type', value_name = 'Prob')
  
  # Write DataFrame with seasons names and their months, and probability DataFrame
  #period.to_csv(output_root+'/seasons.csv', index = False)
  #prob.to_csv(output_root+'/cpt_probabilities.csv', index = False)

  #Return probability DataFrame
  return prob



def forecast_station(station, prob, daily_data_root, output_root, year_forecast, forecast_period):
  
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

  # Read the climate data for the station
  clim = pd.read_csv(daily_data_root + "/"+station +".csv")

  # Filter the probability data for the station
  cpt_prob = prob[prob['id']==station]

  # Get the season for the forecast
  season = cpt_prob['Season'].iloc[0]

  # Adjust the year if the forecast period is 'tri' if necessary
  if forecast_period == 'tri':
    year_forecast = [year_forecast+1 if x in ['DJF']  else year_forecast for x in season][0]
  
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

    if season == 'NDJ':
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
      if season == 'DJF':
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

  # Create folders to save result
  if os.path.exists(output_root + "/"+station):
       output_estacion = output_root + "/"+station
  else:
       os.mkdir(output_root +"/"+ station)
       output_estacion = output_root +"/"+ station

 
  if forecast_period == 'bi':
     if os.path.exists(output_estacion+'/bi/'):
          output_estacion = output_estacion+'/bi/'
     else:
         os.mkdir(output_estacion+'/bi/')
         output_estacion = output_estacion+'/bi/'

  else:
     if os.path.exists(output_estacion+'/tri/'):
        output_estacion = output_estacion+'/tri/'
     else:
       os.mkdir(output_estacion+'/tri/')
       output_estacion = output_estacion+'/tri/'


  # Join seasons samples by column by sample id and save DataFrame in the folder created
  base_years = pd.concat(base_years, axis = 1).rename(columns={'index': 'id'})
  base_years = base_years.iloc[:,[0,1,3] ]
  #base_years.to_csv(output_estacion+ "/samples_for_forecast_"+ forecast_period +".csv", index = False)
  
  # Join climate data filtered for the seasons and save DataFrame in the folder created
  seasons_range = pd.concat(seasons_range).rename(columns={'index': 'id'})

  #Return climate data filtered with sample id 
  return  base_years, seasons_range



def save_forecast(output_root, year_forecast, forecast_period, prob, seasons_range, base_years, station):


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

  # Set the output root based on forecast period
  if forecast_period == 'bi':
     output_estacion = output_root + "/"+station +'/bi/'
  else:
     output_estacion = output_root + "/"+station +'/tri/'


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
      df.to_csv(output_estacion +"_escenario_"+ str(i)+".csv", index=False)

  print("Escenaries saved in {}".format(output_estacion))

  if os.path.exists(output_estacion+ "/summary/"):
      summary_path = output_estacion+ "/summary/"
  else:
      os.mkdir(output_estacion+ "/summary/")
      summary_path = output_estacion+ "/summary/"

  # Calculate maximum and minimum of escenaries by date and save
  df = pd.concat(escenarios)
  df.groupby(['day', 'month']).max().reset_index().sort_values(['month', 'day'], ascending = True).to_csv(summary_path+ station+"_max.csv", index=False)
  df.groupby(['day', 'month']).min().reset_index().sort_values(['month', 'day'], ascending = True).to_csv(summary_path+ station+"_min.csv", index=False)
  print("Minimum and Maximum of escenaries saved in {}".format(summary_path))

