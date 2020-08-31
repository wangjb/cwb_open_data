
import urllib
import requests
from datetime import datetime
import pandas as pd
import ast
import numpy as np
from itertools import islice


def retrieve_data(datacode,auth,shouldraw=False):

  # cwb open data service API 
  url='https://opendata.cwb.gov.tw/'
  sub_path='fileapi/v1/opendataapi/'

  # parameters
  datalimit=''
  dataoffset=''
  dataformat='JSON'
  stationID=''
  datastatus=''

  # create query string
  url_path=url+sub_path+datacode
  query_fields=['Authorization','limit','offset','format','stationID','status']
  query_data=(auth,datalimit,dataoffset,dataformat,stationID,datastatus)
  query_string=dict(zip(query_fields,query_data))

  # request data
  r = requests.get(url_path,params=query_string,stream=True)
  data = r.json() if shouldraw else convert_data(datacode, r.json())

  return data

def convert_data(datacode, jsondata):

  if datacode == 'O-A0059-001':
    return method_O_A0059_001(jsondata)
  elif datacode == 'O-A0002-001':
    return method_O_A0002_001(jsondata)
  elif datacode == 'O-A0038-003':
    return method_O_A0038_003(jsondata)
  elif datacode == 'O-A0003-001':
    return method_O_A0003_001(jsondata)    
  elif datacode == 'O-A0001-001':
    return method_O_A0001_001(jsondata)
  elif datacode == 'F-D0047-071':
    return method_F_D0047_071(jsondata)
  elif datacode == 'C-B0025-001':
    return method_C_B0025_001(jsondata)
  elif datacode == 'C-B0024-001':
    return method_C_B0024_001(jsondata)
  elif datacode == 'M-A0060-024':
    return method_M_A0060_024(jsondata)
  elif datacode == 'M-A0064-024':
    return method_M_A0064_024(jsondata)

def method_O_A0002_001(jsondata):
    data = pd.DataFrame(jsondata['cwbopendata']['location'])

    # process obstime
    data['obstime'] = pd.DataFrame(data['time'].to_list())

    # process weatherElement
    weatherElement = pd.DataFrame(data['weatherElement'].to_list())
    weatherElement.columns = [ x['elementName'] for x in weatherElement.iloc[0] ]
    weatherElement = weatherElement.applymap(lambda x: float(x['elementValue']['value']))

    # process parameter
    parameter = pd.DataFrame(data['parameter'].to_list())
    parameter.columns = [ x['parameterName'] for x in parameter.iloc[0] ]
    parameter = parameter.applymap(lambda x: x['parameterValue'])

    # merge data
    data = data.merge(weatherElement,left_index=True,right_index=True)
    data = data.merge(parameter,left_index=True,right_index=True)
    data = data.drop(['time','weatherElement','parameter'],axis=1)

    return data

def method_O_A0038_003(jsondata):
  data = np.asarray(ast.literal_eval(jsondata['cwbopendata']['dataset']['contents']['content'].replace('\n',',')))
  return data.reshape((120,67))

def method_O_A0003_001(jsondata):
  data = pd.DataFrame(jsondata['cwbopendata']['location'])

  # process obstime
  data['obstime'] = pd.DataFrame(data['time'].to_list())

  # process weatherElement
  weatherElement = pd.DataFrame(data['weatherElement'].to_list())
  weatherElement.columns = [ x['elementName'] for x in weatherElement.iloc[0] ]
  weatherElement = weatherElement.applymap(lambda x: x['elementValue']['value'])

  # process parameter
  parameter = pd.DataFrame(data['parameter'].to_list())
  parameter.columns = [ x['parameterName'] for x in parameter.iloc[0] ]
  parameter = parameter.applymap(lambda x: x['parameterValue'])

  # merge data
  data = data.merge(weatherElement,left_index=True,right_index=True)
  data = data.merge(parameter,left_index=True,right_index=True)
  data = data.drop(['time','weatherElement','parameter'],axis=1)

  return data

def method_C_B0025_001(jsondata):
  data = jsondata['cwbdata']['resources']['resource']['data']['surfaceObs']['location']

  # process station info
  station = pd.DataFrame(list(map(
    lambda x : x['station'],
      data)))
  
  # process stationObsTimes	
  stationObsTime = pd.DataFrame(list(map(
    lambda x : { item.get('dataDate') : item.get('weatherElements').get('precipitation') 
      for item in x['stationObsTimes']['stationObsTime'] },
      data)))

  # process stationObsStatistics	
  stationObsStatistics = pd.DataFrame(list(map(
    lambda x : { item.get('dataYearMonth'):item.get('total') 
      for item in x['stationObsStatistics']['precipitation']['monthly'] },
      data)))
  
  # merge data
  data = station.merge(stationObsTime,left_index=True,right_index=True)
  data = data.merge(stationObsStatistics,left_index=True,right_index=True)
  
  return data

def method_F_D0047_071(jsondata):
  # print out datasetInfo
  print(jsondata['cwbopendata']['dataset']['datasetInfo'])

  cityname = jsondata['cwbopendata']['dataset']['locations']['locationsName']
  towndata = jsondata['cwbopendata']['dataset']['locations']['location']

  # print out city info
  print(cityname)

  # print out obs variable info
  for x in towndata[0]['weatherElement']:
    print(x['description'], x['elementName'])

  
  # process station info
  station = pd.DataFrame(list(map(
         lambda x : dict(islice(x.items(), 4)),
        towndata)))

  # process station data
  stationdata = pd.DataFrame.from_dict({ (i,j) : { item['startTime']+'-'+item['endTime'] : 
                    item['elementValue']['value'] if (j<11 or j>13) else item['elementValue'][0]['value']
                            for item in k['time'] }
          for i,v in enumerate(towndata)
          for j,k in enumerate(v['weatherElement'])
        },orient='Index')
  
  
  return station, stationdata

def method_C_B0024_001(jsondata):

  data = jsondata['cwbdata']['resources']['resource']['data']['surfaceObs']['location']

  # process station info
  station = pd.DataFrame(list(map(
      lambda x : x['station'],
        data)))
  
  # process stationObsTimes	
  stationObsTime = pd.DataFrame.from_dict({ (i,w['dataTime']) : w['weatherElements']
              for i,v in enumerate(data)
              for j,w in enumerate(v['stationObsTimes']['stationObsTime'])
            },orient='Index')
  
  # process stationObsStatistics	
  stationObsStatistics = pd.DataFrame.from_dict({ (i,w['dataDate']) : dict(islice(w.items(), 1, None))
                for i,v in enumerate(data)
                for j,w in enumerate(v['stationObsStatistics']['temperature']['daily'])
              },orient='Index')
  
  return station, stationObsTime, stationObsStatistics

def method_C_B0024_002(jsondata):
  station = None
  return station

def method_O_A0059_001(jsondata):

  # print data info 
  print(jsondata['cwbopendata']['dataset']['datasetInfo'])

  # transforma data
  data = np.asarray(ast.literal_eval(jsondata['cwbopendata']['dataset']['contents']['content'])).reshape((881,921))

  return data

def method_O_A0001_001(jsondata):

  data = pd.DataFrame(jsondata['cwbopendata']['location'])

  # process obstime
  data['obstime'] = pd.DataFrame(data['time'].to_list())

  # process weatherElement
  weatherElement = pd.DataFrame(data['weatherElement'].to_list())
  weatherElement.columns = [ x['elementName'] for x in weatherElement.iloc[0] ]
  weatherElement = weatherElement.applymap(lambda x: x['elementValue']['value'])

  # process parameter
  parameter = pd.DataFrame(data['parameter'].to_list())
  parameter.columns = [ x['parameterName'] for x in parameter.iloc[0] ]
  parameter = parameter.applymap(lambda x: x['parameterValue'])

  # merge data
  data = data.merge(weatherElement,left_index=True,right_index=True)
  data = data.merge(parameter,left_index=True,right_index=True)
  data = data.drop(['time','weatherElement','parameter'],axis=1)

  return data

def method_M_A0060_024(jsondata):
  # print out data info
  print(jsondata['cwbopendata']['dataset']['datasetInfo']['parameterSet'])

  # return GFS data url
  return jsondata['cwbopendata']['dataset']['resource']['uri']

def method_M_A0064_024(jsondata):
  # print out data info
  print(jsondata['cwbopendata']['dataset']['datasetInfo']['parameterSet'])

  # return GFS data url
  return jsondata['cwbopendata']['dataset']['resource']['uri']