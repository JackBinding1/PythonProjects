#-------------------------------------------------------------------------------------------------------------------
#- Program : JSONHandler.py
#- Language : Python 3.8
#- Author : Jack Binding
#- Date : 08-04-2024
#- Purpose : Creation of functions to help handle .json files
#-------------------------------------------------------------------------------------------------------------------
#- Version History:
#- Date Developer Change Details
#- --------- --------------- ---------------------------------------------------------------------------------------
#- ddMONyyyy Your Name      Brief Detail of Change
#- 10APR2024 Jack Binding   Added items[f"{new_key}_{ColUnique}_{i}"] = item (*1). Removed underscore after [0-9] (*2)
#-------------------------------------------------------------------------------------------------------------------

from json import load
from datetime import datetime
from pandas import DataFrame,concat
from re import search,sub
from numpy import repeat, where
ColUnique = 'ffjnkvFoipslkj' # referenced in flatten_json(), used to help keep track of nested lists within dicts.


# defining file paths & file
path = 'C://Users/jack.binding/Documents/SnowflakePython/'
file = 'Facebook-Dusk-AdStats-FacebookInsights-20240406-080034.json'

file_no_ext = file.split('.')[0]

f = open(path+file)
data = load(f) # returns JSON object as a dictionary or list


# defining file paths & file
date_time_for_file = str(datetime.now()).split('.')[0].replace('-','').replace(':','').replace(' ','_')
file_out = f'{path}{file_no_ext}_{date_time_for_file}.csv'


# creating functions - START

def flatten_json(json_obj, parent_key=''):
   items = {}
   for key, value in json_obj.items():
       new_key = f"{parent_key}_{key}" if parent_key else key
       if isinstance(value, dict):
           items.update(flatten_json(value, new_key))
       elif isinstance(value, list):
           for i, item in enumerate(value):
               if isinstance(item, dict):
                   items.update(flatten_json(item, f"{new_key}_{ColUnique}_{i}"))
               else:
                   items[f"{new_key}_{ColUnique}_{i}"] = item # (*1)
       else:
           items[new_key] = value
   return items



def DictToPD(d):
    # to parse dict to panda's dataframe, you need to apply an index..
    # or you can simply put all values within a list and the DataFrame will handle it
    df_prep = dict()
    for k,v in d.items():
            df_prep[k] = [v]
    df_json = DataFrame(df_prep)

    # now to split fields that contain ColUnique and those that do not
    main = []
    side = []

    for i in list(df_json.columns):
        if ColUnique in i:
            side.append(i)
        else:
            main.append(i)
    

    # splitting df_json into two DataFrames
    df_main = df_json[main]
    df_side = df_json[side]

    # re-labelling fields so there are col clashes. This is to stack records later on
    x = [sub(f'{ColUnique}_[0-9]','',i) for i in side if search('_[0-9]',i)] # *2
    df_side.columns = x
    x_dedupe =  list(set(x))


    print('\n\nx:',x,'\ndf_side:',df_side)


     # create empty dataframe with appropriate columns
    df_side_shaped = DataFrame(columns=x_dedupe)

    # since > 1 column name can be col - this grabs all of records, stacks then, assigns them to that column
    for col in df_side:
        df_side_shaped[col] = list(df_side.loc[0,col])

    # help to see the reshape of df_side. Transpose-ish
    if len(df_side) > 0:
      print('df_side\n',df_side.shape,'\ndf_side_shaped\n',df_side_shaped.shape)


    # explode df_main to have the number of rows as df_side,bring forward the column names from df_main and merge in df_side and return the result dataframe
    df_main_explode = DataFrame(repeat(df_main.values, where(len(df_side_shaped.index) >0,len(df_side_shaped.index),1), axis=0),columns= df_main.columns)
    df_all = concat([df_main_explode, df_side_shaped], axis=1,join='outer')
    return df_all



# creating functions - END




print('Loaded fileType:',type(data),'\n')




if isinstance(data,dict):
    # Flatten the JSON object which returns a dictionary
    flattened_json = flatten_json(data)
    df = DictToPD(flattened_json)
    df.to_csv(file_out, encoding='utf-8',index=False,line_terminator='\n')
    exit()


df_combined = DataFrame()
if isinstance(data,list):
    for d in data: # assuming here it is a list with Dicts inside. Like a file containing multiple JSON records
        flattened_json = flatten_json(d)
        df = DictToPD(flattened_json)
        if len(df_combined) == 0:
            df_combined = df
        else:
            df_combined = concat([df_combined,df],axis=0)


df_combined.to_csv(file_out, encoding='utf-8',index=False,line_terminator='\n')
