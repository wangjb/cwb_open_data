
import urllib
import requests

def retrieve_data(datacode,auth):

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

  return r.json()