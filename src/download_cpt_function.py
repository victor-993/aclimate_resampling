import pandas as pd
import numpy as np
import os
import requests
import gzip

# Define a function to check if the gzip file is valid
def is_gzip_valid(file_path):
     with gzip.open(file_path, 'rb') as f:
          try:
              f.read()
              return True
          except Exception as e:
              return False
          
def download_cpt(dir_save, areas_l, n_areas_l, month, year,  gunzip_test_val = 1):


    season = np.arange(month, month + 6)
    y = np.repeat(year, len(season))
    y_season = np.where(season > 12, y + 1, y)
    season[season > 12] -= 12
    season[season < 1] += 12

    season_for = season[[1, 4]]
    areas = [areas_l[season_for[0]], areas_l[season_for[1]]]
    n_areas = [n_areas_l[season_for[0]], n_areas_l[season_for[1]]]
    month_abb = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    i_con = month - 1
    if i_con <= 0:
        i_con += 12

    for i in range(2):
        t = [1, 4]
        if n_areas[i] == 4:
            route = f"http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP/.EMC/.CFSv2/.ENSEMBLE/.OCNF/.surface/.TMP/SOURCES/.NOAA/.NCEP/.EMC/.CFSv2/.REALTIME_ENSEMBLE/.OCNF/.surface/.TMP/appendstream/350/maskge/S/%280000%201%20{month:02d}%201982-{y_season[t[i]]}%29/VALUES/L/{t[i]}.5/{t[i] + 2}.5/RANGE/%5BL%5D//keepgrids/average/M/1/24/RANGE/%5BM%5Daverage/X/{areas[i][0]}/{areas[i][1]}/flagrange/Y/{areas[i][2]}/{areas[i][3]}/flagrange/add/1/flaggt/mul/-999/setmissing_value/%5BX/Y%5D%5BS/L/add/%5Dcptv10.tsv.gz"
        else:
            route = f"http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP/.EMC/.CFSv2/.ENSEMBLE/.OCNF/.surface/.TMP/SOURCES/.NOAA/.NCEP/.EMC/.CFSv2/.REALTIME_ENSEMBLE/.OCNF/.surface/.TMP/appendstream/350/maskge/S/%280000%201%20{month:02d}%201982-{y_season[t[i]]}%29/VALUES/L/{t[i]}.5/{t[i] + 2}.5/RANGE/%5BL%5D//keepgrids/average/M/1/24/RANGE/%5BM%5Daverage/X/{areas[i][0]}/{areas[i][1]}/flagrange/Y/{areas[i][2]}/{areas[i][3]}/flagrange/add/1/flaggt/X/{areas[i][4]}/{areas[i][5]}/flagrange/Y/{areas[i][6]}/{areas[i][7]}/flagrange/add/1/flaggt/add/mul/-999/setmissing_value/%5BX/Y%5D%5BS/L/add/%5Dcptv10.tsv.gz"


        file_name =  f"{dir_save}/{i}_{'_'.join([month_abb[m - 1] for m in season[t[i]:(t[i] + 2)]])}.tsv.gz"

        # Download the file
        while gunzip_test_val == 1:
            response = requests.get(route)

            with open(file_name, 'wb') as f:
                f.write(response.content)

            # Check if the gzip file is valid
            gunzip_test_val = not is_gzip_valid(file_name)

            if gunzip_test_val == 1:
                print('... gzip file is corrupted, retrying download\n\n\n')
            else:
                print(file_name)
                print('... file downloaded successfully\n\n\n')

# Example usage
#dir_save = "path/to/your/save/directory"
#areas_l = {1: [0, 1, 2, 3, 4, 5, 6, 7], 5: [8, 9, 10, 11, 12, 13, 14, 15]}
#n_areas_l = {1: 4, 5: 8}
#month = 1
#year = 2023

download_cpt(dir_save, areas_l, n_areas_l, month, year)
