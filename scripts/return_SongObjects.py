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
import sys

def main():
    """

    """
    parser = argparse.ArgumentParser(description='Retrieve UNPUBLISHED and SUPPRESSED objects from SONG API')
    parser.add_argument('-p', '--project', dest="project", help="projects to query",type=str,nargs="+",default=['all'])
    parser.add_argument('-u', '--url', dest="song_url", help="SONG", required=True,type=str)
    parser.add_argument('-o', '--output_directory', dest="out_dir", help="Output Directory", default=os.getcwd(),type=str)
    parser.add_argument('-s', '--state', dest="state", help="analysis state to query",default=["UNPUBLISHED","SUPPRESSED"],choices=["PUBLISHED","UNPUBLISHED","SUPPRESSED"],nargs="+")

    cli_input= parser.parse_args()

    checkSongUrl(cli_input.song_url)
    objects_to_remove=setDataFrame()

    if "all" in cli_input.project:
        studies=getStudyIDs(cli_input.song_url)
    else:
        studies=cli_input.project

    for study in studies:
        for state in cli_input.state:
            querySong(cli_input.song_url,study,state,objects_to_remove)

    objects_to_remove.to_csv(
        "%s/%s-%s-RemovableObjects.tsv" % (
            cli_input.out_dir,
            datetime.date.today().strftime("%d_%m_%Y"),
            ".".join(cli_input.song_url.split("/")[-1].split(".")[:-1])
            ),
        sep='\t'
        )

def checkSongUrl(song_url):
    url="%s/isAlive" % (song_url)

    response=requests.get(url)

    if response.status_code!=200:
        sys.exit("isAlive endpoint not working for '%s', status code: %s" % (song_url,str(response.status_code)))
    if not response.json():
        sys.exit("'%s' is not alive, panic." % (song_url))

    print("%s is alive!" % (song_url))
    return True

def setDataFrame():
    return pd.DataFrame(columns=['fileName','fileSize','fileType','objectId','analysisId','studyId','analysisState'])

def getStudyIDs(song_url):
    url="%s/studies/all" % (song_url)
    #print(url)
    response=requests.get(url)
    #print(response.status_code)
    if response.status_code!=200:
        sys.exit("GetAllStudyIds endpoint not working for '%s', status code: %s" % (song_url,str(response.status_code)))
    if len(response.json())==0:
        sys.exit("GetAllStudyIds endpoint return zero hits '%s'" % (song_ur))
    
    print("All specified. Retrieving Song IDs")
    return response.json()

def querySong(song_url,study,state,objects_to_remove):
    url="%s/studies/%s/analysis?analysisStates=%s" % (song_url,study,state)

    response=requests.get(url)

    if response.status_code!=200:
        sys.exit("Following query '%s' errored, status code: %s" % (url,str(response.status_code)))

    print("Searching %s %s" % (state,study))
    if len(response.json())>0:
        for analysis in response.json():
            for file in analysis['files']:
                count=len(objects_to_remove)
                objects_to_remove.loc[count,"fileName"]=file["fileName"]
                objects_to_remove.loc[count,"fileSize"]=file["fileSize"]
                objects_to_remove.loc[count,"fileType"]=file["fileType"]
                objects_to_remove.loc[count,"objectId"]=file["objectId"]
                objects_to_remove.loc[count,"analysisId"]=analysis['analysisId']
                objects_to_remove.loc[count,"studyId"]=analysis['studyId']
                objects_to_remove.loc[count,"analysisVersion"]=analysis['analysisType']['version']
                objects_to_remove.loc[count,"analysisType"]=analysis['analysisType']['name']
                objects_to_remove.loc[count,"analysisState"]=analysis['analysisState']
    
    return objects_to_remove

if __name__ == "__main__":
    main()