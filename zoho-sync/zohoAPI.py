import os
import pandas as pd
import numpy as np
import re
from pandas.tseries.offsets import MonthEnd
from datetime import timedelta
from datetime import datetime
import time
import itertools
import sqlite3
import requests
from requests.auth import HTTPBasicAuth
import csv
from io import StringIO

import xml.etree.ElementTree as ET

# Zoho API data
class ZohoAPI:
    def __init__(self, owner, auth_token, application_name):
        self.owner = owner
        self.auth_token = auth_token
        self.application_name = application_name
        self.scope = 'creatorapi'
        self.format_type = 'json'
        self.APIheader = {
            'authtoken': self.auth_token, 
            'scope': self.scope
        } 
        self.RPCheader = {
            **self.APIheader,
            **{'zc_ownername': self.owner}
        } 
        self.baseUrl = 'https://creator.zoho.com/api'
        self.addUrl = '/record/add'
        self.deleteUrl = '/record/delete'
        self.editUrl = '/record/update'
        self.rpcUrl = '/xml/write' 
        #Create xml base for Zoho RPC API - HOW DO YOU HAVE NEW LINES IN CODE AND THEN REMOVE,SO YOU DON'T HAVE THESE LONG LINES THAT GO OVER THE PAGE
        self.rpcBaseXML = """<ZohoCreator><applicationlist><application name={0}><formlist><form name={1}>{2}</form></formlist></application></applicationlist></ZohoCreator>"""
        
    #  function to add single row
    def add(self, form, payload):
        url = self.baseUrl + '/' + self.owner + '/' + self.format_type + '/' + self.application_name + '/' + 'form/' + form + self.addUrl 
        headers = {**self.APIheader,**payload}
        r = requests.post(url, data = headers)
        return(r)
    
    #  function to delete based on condition
    def delete(self, form, condition):
        url = self.baseUrl + '/' + self.owner + '/' + self.format_type + '/' + self.application_name + '/' + 'form/' + form + self.deleteUrl 
        headers = {**self.APIheader,**{"criteria":condition}}
        r = requests.post(url, data = headers)
        return(r)
    
    # function to add many rows at once
    def rpcAdd(self, xml):
        payload =  {**self.RPCheader,**{"XMLString":xml}}
        r = requests.post(self.baseUrl + self.rpcUrl, data = payload)
        return(r)
    
    # function to create the xml structure for the rpc
    def createXml(self, form, data):
        ### WARNING - NOT SURE OF MAX LENGTH YET
        # Create a series of formatted xml strings of the data
        xml_data = data.apply(self._pandas_xml_func, axis=1)
        #Join every row in the series to create one long xml
        xml_data = xml_data.str.cat(sep='')
        # Add to the xml base format 
        xml = self.rpcBaseXML.format('"'+self.application_name+'"','"'+form+'"', xml_data)        
        return(xml)
    
    def _pandas_xml_func(self, row):
        data_xml = ['<add>']
        for field in row.index:
            if row[field]!='':
                data_xml.append('<field name="{0}"><value>{1}</value></field>'.format(field, row[field]))
        data_xml.append('</add>')
#         print("".join(data_xml))
        return("".join(data_xml))
        
#     def view(self, )


# function to delete a form from zoho account
def formDelete(form, zoho):
    print("Deleting...")
    delete_request = zoho.delete(form,'ID != null') #ID is never null so should delete all
    if delete_request.status_code == 200:
        print("Success: " + form + " has been deleted")
        print(delete_request.text)
        
    else:
        print("Error. See status code:")
        print(delete_request.status_code)

    # Wait 10 seconds to let the delete trigger.
    time.sleep(10)
    return(delete_request)        

# General syncronous method
# The XML RPC API can probably take up 1.4m. characters maximum.
# Have to make sure the slice length use keeps each xml under this limit then

# the relationship depends on # columns, # columns with values etc.

# you can probably slim down the payload by only submitting columns with values - may be lots of blanks - but still limited
# The return value is a list, each value in the list is a JSON structured as {response:{status:<status_code>, text:<XMLresponse>}}
def dfUploadSync(df, form, zoho, slice_length=500):
    responses = []
    response_count = 0
    
    # Seems like 500 is the limit - do it in slices
    for _, df_slice in df.groupby(np.arange(len(df)) // slice_length):
        print("Request number: " + str(response_count) + ", Records: " + str(len(df_slice)))
        # Create xml string from accounts data
        xml_string = zoho.createXml(form, df_slice)

        # Send query to Zoho
        rpc_request = zoho.rpcAdd(xml_string)
        responses.append({response_count:{"status":rpc_request.status_code, "text":rpc_request.text}})
        response_count += 1
        
        # Standard output results 
        # First check if request is successfull
        if rpc_request.status_code==200:
            # Check if the response text has an error list
            if "errorlist" not in rpc_request.text:
                # Every row has a response and you can check it's status in the XML response
                root=ET.fromstring(rpc_request.text)
                for status in root.iter('status'):
                    if status.text != 'Success':
                        print("WARNING: " + status.text)
            else:
                print(rpc_request.text)
                raise Exception("Received errorlist in rpc response from Zoho")

        else:
            print("Error: " + str(rpc_request.status_code) + " - see rpc request text for more detail")
            print(rpc_request.text)
            raise ValueError("Request number " + str(response_count) + " failed")
        
        # Wait 5 seconds after each request to give it time to update in Zoho - UNKNOWN IF NEEDED
        # time.sleep(5)

    print("Completed request.")       
    return(responses)
