# -*- coding: utf-8 -*-

import pandas as pd
import calendar
import numpy as np
import random
from statistics import mode
import os
import warnings
warnings.filterwarnings("ignore")


ruta_daily_data = "/content/drive/MyDrive/ANGOLA/inputs/prediccionClimatica/dailyData"
ruta_probabilidades = "/content/drive/MyDrive/ANGOLA/outputs/probForecast/probabilities.csv"
ruta_salidas = "/content/drive/MyDrive/ANGOLA/save"

def preprocessing(prob_root,  output_root, forecast_period):

  months_names = list(calendar.month_name)[1:]
  prob = pd.read_csv(prob_root)
  prob['below'] = prob['normal'] = prob['above'] = 1/3

  if forecast_period == "tri":
    months = ''.join([x[0] for x in months_names])+'JF'
    months_numbers =list(range(1,13)) + [1,2]
    period_numbers = pd.DataFrame( [months_numbers[i:i+3] for i in range(0, len(months_numbers)-2)])
    period = pd.DataFrame([months[i:i+3] for i in range(0, len(months))][:-2])
    period = pd.concat([period, period_numbers], axis = 1)
    period.columns = ['Season','Start', 'Central_month', 'End']
    prob = prob.merge(period, left_on='month', right_on='Central_month')


  else:
    if forecast_period == "bi":
      months = ''.join([x[0] for x in months_names])
      months_numbers = list(range(1,13))
      period_numbers = pd.DataFrame( [months_numbers[i:i+2] for i in range(0, len(months_numbers),2)])
      period = pd.DataFrame([months[i:i+2] for i in range(0, len(months),2)])
      period = pd.concat([period, period_numbers], axis = 1)
      period.columns = ['Season','Start', 'End']
      prob_a = prob.merge(period, left_on='month', right_on='Start')
      prob = prob_a.append(prob.merge(period, left_on='month', right_on='End'))


  prob.drop(['month'], axis = 1, inplace = True )
  prob = prob.melt(id_vars = ['year', 'id', 'Season', 'Start','End'], var_name = 'Type', value_name = 'Prob')
  period.to_csv(output_root+'/seasons.csv', index = False)
  prob.to_csv(output_root+'/cpt_probabilities.csv', index = False)
  return prob



def forecast_station(station, prob, daily_data_root, output_root, year_forecast, forecast_period):

  clim = pd.read_csv(daily_data_root + "/"+station +".csv")
  cpt_prob = prob[prob['id']==station]
  season = cpt_prob['Season'].iloc[0]

  if forecast_period == 'tri':
    year_forecast = [year_forecast+1 if x in ['DJF']  else year_forecast for x in season][0]
  
  leap_forecast = (year_forecast%400 == 0) or (year_forecast%4==0 and year_forecast%100!=0)

    
  clim_feb = clim.loc[clim['month'] == 2]
  clim_feb['leap'] = [True if (year%400 == 0) or (year%4==0 and year%100!=0) else False for year in clim_feb['year']]

  february = []
  for i in np.unique(clim_feb['year']):
    year_data =  clim_feb.loc[clim_feb['year']==i,:]
    year = year_data.loc[:,'leap']
    if leap_forecast == True and year.iloc[0] == False:
      year_data.append(year_data.sample(1), ignore_index=True)
      year_data.iloc[-1,0] = 29
    else:
      if leap_forecast == False and year.iloc[0] == True:
         year_data =  year_data.iloc[:-1]
      else:
        year_data = year_data
  february.append(year_data)

  data = pd.concat(february).drop(['leap'], axis = 1 )
  data = pd.concat([data,clim.loc[clim['month'] != 2]]).sort_values(['year','month'])


  base_years = []
  seasons_range = []
  for season in  list(np.unique(cpt_prob['Season'])):
    x = cpt_prob[cpt_prob['Season'] == season] 

    if x['Start'].iloc[0] > x['End'].iloc[0]:
        data_range = data.loc[data['month'] >= x['Start'].iloc[0]]
        data_range_2 = data.loc[data['month'] <= x['End'].iloc[0]]
        data_range = data_range.append(data_range_2)

    else:
        data_range = data.loc[(data['month'] >= x['Start'].iloc[0]) & (data['month'] <= x['End'].iloc[0])]

    new_data = data_range[['year','prec']].groupby(['year']).sum().reset_index() 
    cuantiles = list(np.quantile(new_data['prec'], [.33,.66]))
    new_data['condition'] =  'NA'
    new_data.loc[new_data['prec']<= cuantiles[0], 'condition'] = 'below'
    new_data.loc[new_data['prec']>= cuantiles[1], 'condition'] =  'above'
    new_data.loc[(new_data['prec']> cuantiles[0]) & (new_data['prec']< cuantiles[1]), 'condition'] =  'normal'
    merge = pd.merge(data_range,new_data[['condition', 'year']],on='year', how = 'left')
    merge['Season'] = season
    merge = merge.drop(['condition'], axis =1)
    muestras = x[['Start', 'End', 'Type', 'Prob']].sample(100, replace = True, weights=x['Prob'])
    muestras = muestras.set_index(pd.Index(list(range(0,100))))
    muestras_by_type = []
    for i in muestras.index: 
      m = new_data.loc[new_data['condition'] == muestras['Type'].iloc[i]].sample(1)
      muestras_by_type.append(m) 
    muestras_by_type = pd.concat(muestras_by_type).reset_index()
    muestras_by_type['index'] = muestras.index
    muestras_by_type = muestras_by_type.set_index(pd.Index(list(range(0,100))))
      
    muestras_by_type = muestras_by_type.rename(columns = {'year':season})
    muestras_by_type['plus'] = list(map(lambda x: x + 1, muestras_by_type[season]))
    years = list(muestras_by_type[season])
    years.sort()

    if season == 'NDJ':
      
      years_plus = list(map(lambda x: x + 1, years))
      years_plus.sort()
    
      months_numbers =[11,12]

      merge_a = merge[merge['year'].isin(years)]
      merge_a = merge_a[merge_a['month'].isin(months_numbers)]
      merge_a = pd.merge(merge_a, muestras_by_type[['index', season]], left_on = 'year', right_on = season)
      merge_a.drop(season, axis = 1,inplace = True)


      merge_b = merge[merge['year'].isin(years_plus)]
      merge_b = merge_b[merge_b['month'] == 1]
      merge_b = pd.merge(merge_b, muestras_by_type[['index', 'plus']], left_on = 'year', right_on = 'plus')
      merge_b.drop('plus', axis = 1,inplace = True)

      merge = merge_a.append(merge_b)
        
    else:
      if season == 'DJF':
        years_plus = list(map(lambda x: x + 1, years))
        years_plus.sort()
        months_numbers =[1,2]
        merge_a = merge[merge['year'].isin(years)]
        merge_a = merge_a[merge_a['month'] == 12]
        merge_a = pd.merge(merge_a, muestras_by_type[['index', season]], left_on = 'year', right_on = season)
  
    
        merge_b = merge[merge['year'].isin(years_plus)]
        merge_b = merge_b[merge_b['month'].isin(months_numbers)]
        merge_b = pd.merge(merge_b, muestras_by_type[['index', 'plus']], left_on = 'year', right_on = 'plus')
        merge_b.drop('plus', axis = 1,inplace = True)


        merge = merge_a.append(merge_b)

      else:
        merge = merge.loc[merge['year'].isin(muestras_by_type[season])]
        merge = pd.merge(merge,muestras_by_type[['index',season]],left_on = 'year', right_on = season)
        merge = merge.drop(season, axis = 1)


    base_years.append(muestras_by_type[['index',season]])
    seasons_range.append(merge)

  if os.path.exists(output_root + "/"+station):
       output_estacion = output_root + "/"+station
  else:
       output_estacion = os.mkdir(output_root +"/"+ station)
       output_estacion = str(output_estacion)

 
  if forecast_period == 'bi':
     if os.path.exists(output_estacion+'/bi/'):
          output_estacion = output_estacion+'/bi/'
     else:
          output_estacion = os.mkdir(output_estacion+'/bi/')
  else:
     if os.path.exists(output_estacion+'/tri/'):
        output_estacion = output_estacion+'/tri/'
     else:
        output_estacion = os.mkdir(output_estacion+'/tri/')
        output_estacion = str(output_estacion)


  



  base_years = pd.concat(base_years, axis = 1).rename(columns={'index': 'id'})
  base_years = base_years.iloc[:,[0,1,3] ]
  base_years.to_csv(output_estacion+ "/samples_for_forecast_"+ forecast_period +".csv", index = False)
  seasons_range = pd.concat(seasons_range).rename(columns={'index': 'id'})
  return base_years, seasons_range



def save_forecast(output_root, year_forecast, forecast_period, prob, seasons_range, base_years, station):

  if forecast_period == 'bi':
     output_estacion = output_root + "/"+station +'/bi/'
  else:
     output_estacion = output_root + "/"+station +'/tri/'



  cpt_prob = prob[prob['id']==station]

  year_forecast = [year_forecast+1 if x in ['NDJ', 'DJF']  else year_forecast for x in cpt_prob['Season'].iloc[0]][0]

  escenarios = []
  for i in base_years.index:
      df = seasons_range[(seasons_range['id'] == base_years['id'].iloc[i])]
      df['year'] = year_forecast
      df = df.drop(['id', 'Season'], axis = 1)
      escenarios.append(df)
      df.to_csv(output_estacion +"_escenario_"+ str(i)+".csv", index=False)

  if os.path.exists(output_estacion+ "/summary/"):
      summary_path = output_estacion+ "/summary/"
  else:
      summary_path = os.mkdir(output_estacion+ "/summary/")
      summary_path = str(summary_path)


  df = pd.concat(escenarios)
  df.groupby(['day', 'month']).max().reset_index().sort_values(['month', 'day'], ascending = True).to_csv(summary_path+ station+"_max.csv", index=False)
  df.groupby(['day', 'month']).min().reset_index().sort_values(['month', 'day'], ascending = True).to_csv(summary_path+ station+"_min.csv", index=False)

  return(df)
