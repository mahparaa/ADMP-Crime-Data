
import os
import requests
import pandas as pd

dir1 = "C:\\Users\\Mahpara\\ADMP\\crime-data\\crime\\outcomes"
url = "https://api.postcodes.io/postcodes"

database = {}

def fetch_results(root_dirs):
    for root_d in root_dirs:
        for path, subdirs, files in os.walk(root_d):
            list_of_regions = []
            for name in files:
                dataset = os.path.join(path, name)
                df = pd.read_csv(dataset)
                geocodes = list(zip(df.Longitude, df.Latitude, df['LSOA code']))
                for longi, lati, lso_code in geocodes:
                    if lso_code in database:
                        print('Data already fetched')
                        continue
                        
                    if not "wales" in dataset:
                        print("skipping countries except wales")
                        continue
                        
                    parameterize_url = url + "?longitude=" + str(longi) + "&latitude=" + str(lati)

                    response = requests.get(parameterize_url)
                    if response.status_code >= 400:
                        print('Empty/Missing Data found skipping')
                        continue

                    print('Fetching new data ' + parameterize_url + ' filename: ' + dataset)
                    data = response.json()

                    if data['result'] is None:
                        print('Result found None')
                        continue

                    if 'result' in data:
                        database[lso_code] = True
                        result = data['result'][0]
                        
                        collection = { "lsoa": lso_code, "region": result["region"], "country": result["country"]}
                        
                        if result["region"]:
                            collection["region"] = result["region"]
                        else:
                            collection["region"] = result["european_electoral_region"]
                            
                        list_of_regions.append(collection)

                print('Saving to ' + name)
                df = pd.DataFrame(list_of_regions)
                df.to_csv(name)    
                print('Done!')


fetch_results([dir1])