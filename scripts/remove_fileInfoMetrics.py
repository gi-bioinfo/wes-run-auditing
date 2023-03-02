#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
  Copyright (C) 2022,  icgc-argo

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

  Authors:
    Edmund Su
"""

import requests
import pandas as pd
import argparse
import datetime
import os
import json
import sys
import glob

def main():
    """

    """
    parser = argparse.ArgumentParser(description='Retrieve UNPUBLISHED and SUPPRESSED objects from SONG API')
    parser.add_argument('-t', '--token', dest="token", help="token",type=str,required=True)
    parser.add_argument('-u', '--url', dest="song_url", help="SONG", required=True,type=str)
    parser.add_argument('-o', '--output_directory', dest="out_dir", help="Output directory to save", default=os.getcwd(),type=str)
    parser.add_argument('-c', '--csv', dest="csv", help="CSV with object IDs to parse; columns should be : object-id,study-id",required=True)

    cli_input= parser.parse_args()

    checkSongUrl(cli_input.song_url)

    objects_df=parseCsv(cli_input.csv)
    objects_dict=song_get_metadata(objects_df,cli_input.song_url)

    save_metadata(cli_input.out_dir+"/original",objects_dict)

    altered_dict=clear_metrics(objects_dict)
    song_update_metadata(altered_dict,cli_input.song_url,cli_input.token)

    #updated_dict=song_get_metadata(objects_df,cli_input.song_url)
    #save_metadata(cli_input.out_dir,updated_dict)
    #compare(objects_df,cli_input.out_dir)

def parseCsv(csv):
    df=pd.read_csv(csv,sep=',',names=["object_id","study_id"])
    #print(df)
    if len(df.columns.values.tolist())!=2:
        sys.exit("Invalid CSV format")
    else:
        return df

def song_get_metadata(objects_df,song_url):
    object_dict={}
    for ind in objects_df.index.values.tolist():
        study_id=objects_df.loc[ind,"study_id"]
        object_id=objects_df.loc[ind,"object_id"]
        url="%s/studies/%s/files/%s" % (song_url,study_id,object_id)
        #https://song.rdpc.cancercollaboratory.org/studies/OCCAMS-GB/files/ad24a0c7-db3d-5ac6-9b03-3e89961973ea
        response=requests.get(url)

        if response.status_code!=200:
            sys.exit("Following query '%s' errored, status code: %s" % (url,str(response.status_code)))
        else:
            object_dict[object_id]=response.json()
    
    print("%s objects found!" % (str(len(object_dict.keys()))))
    
    return object_dict

def save_metadata(out_dir,objects_dict):
    if not(os.path.exists(out_dir)):
        os.mkdir(out_dir)
    #print(objects_dict)
    for key in objects_dict.keys():
        filepath="%s/%s.json" % (out_dir,key)
        with open(filepath, 'w') as f:
            json.dump(objects_dict[key], f, indent=2)

def clear_metrics(objects_dict):
    ###Assuming metrics is nested within info
    for key in objects_dict:
        objects_dict[key]["info"]['metrics']={}
    return objects_dict

def song_update_metadata(altered_dict,song_url,token):
    object_dict={}
    for key in altered_dict.keys():
        study_id=altered_dict[key]['studyId']
        object_id=altered_dict[key]['objectId']
        data=altered_dict[key]
        headers = {
        'accept': '*/*',
        'Authorization': 'Bearer %s' % (token),
        'Content-Type': 'application/json',
        }
        url="%s/studies/%s/files/%s" % (song_url,study_id,object_id)
        #https://song.rdpc.cancercollaboratory.org/studies/OCCAMS-GB/files/ad24a0c7-db3d-5ac6-9b03-3e89961973ea
        response= requests.put(
            url,
            headers=headers,
            json=data,
            )

        if response.status_code!=200:
            sys.exit("Following query '%s' errored, status code: %s" % (url,str(response.status_code)))
        else:
            print("%s updated!" % (url))


def compare(objects_df,out_dir):
    print("hello")

def checkSongUrl(song_url):
    url="%s/isAlive" % (song_url)

    response=requests.get(url)

    if response.status_code!=200:
        sys.exit("isAlive endpoint not working for '%s', status code: %s" % (song_url,str(response.status_code)))
    if not response.json():
        sys.exit("'%s' is not alive, panic." % (song_url))

    print("%s is alive!" % (song_url))
    return True

if __name__ == "__main__":
    main()