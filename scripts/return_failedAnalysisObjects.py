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
    parser.add_argument('-s', '--song_url', dest="song_url", help="SONG URL", required=True,type=str)
    parser.add_argument('-g', '--graphql_url', dest="rdpc_url", help="GRAPHQL URL", required=True,type=str)
    parser.add_argument('-o', '--output_directory', dest="out_dir", help="Output Directory", default=os.getcwd(),type=str)
    parser.add_argument('-t', '--token', dest="token", help="API token",type=str,required=True)
    parser.add_argument('-r', '--repos', dest="repos", help="Additional repository",type=str,nargs="*",default=[])

    cli_input= parser.parse_args()

    # checkSongUrl(cli_input.song_url)
    objects_to_remove=setDataFrame()

    base_repos=[
        'https://github.com/icgc-argo-workflows/dna-seq-processing-wfs',
        'https://github.com/icgc-argo-workflows/open-access-variant-filtering',
        'https://github.com/icgc-argo-workflows/rna-seq-alignment',
        'https://github.com/icgc-argo/dna-seq-processing-wfs.git',
        'https://github.com/icgc-argo/gatk-mutect2-variant-calling.git',
        'https://github.com/icgc-argo/icgc-25k-azure-transfer.git',
        'https://github.com/icgc-argo/sanger-wgs-variant-calling.git',
        'https://github.com/icgc-argo/sanger-wxs-variant-calling.git',
    ]
    repos=base_repos+cli_input.repos
    for repo in repos:
        for state in ["EXECUTOR_ERROR","SYSTEM_ERROR"]:
            queryRDPC(cli_input.rdpc_url,cli_input.token,repo,state,objects_to_remove)
            
    verify_song_objects(cli_input.song_url,objects_to_remove)

    objects_to_remove.to_csv(
        "%s/%s-%s-RemovableObjects.tsv" % (
            cli_input.out_dir,
            datetime.date.today().strftime("%d_%m_%Y"),
            cli_input.rdpc_url.replace("https://","").replace("/graphql","")
            ),
        sep='\t'
        )

def verify_song_objects(song_url,objects_to_remove):
    for ind in objects_to_remove.index.values.tolist():
        #https://song.rdpc-qa.cancercollaboratory.org/studies/TEST-PR/files/b26b4f92-70eb-5762-bc68-3c0c07357c81
        url="%s/studies/%s/files/%s" % (song_url,objects_to_remove.loc[ind,"studyId"],objects_to_remove.loc[ind,"objectId"])
        response=requests.get(url)
        if response.status_code!=200 and response.status_code!=404:
            sys.exit("Following query '%s' errored, status code: %s" % (url,str(response.status_code)))
        elif response.status_code==404:
            objects_to_remove.loc[ind,"removable"]=False
        elif response.status_code==200:
            objects_to_remove.loc[ind,"removable"]=True
            response=check_analysis(song_url,objects_to_remove.loc[ind,"studyId"],objects_to_remove.loc[ind,"analysisId"])
            objects_to_remove.loc[ind,"analysisState"]=response.json()['analysisState']
        else:
            sys.exit("Bad scenario")
    return objects_to_remove

def check_analysis(song_url,studyId,analysisId):
    url="%s/studies/%s/analysis/%s" % (song_url,studyId,analysisId)
    response=requests.get(url)
    if response.status_code!=200:
        sys.exit("Following query '%s' errored, status code: %s" % (url,str(response.status_code)))
    return response

def setDataFrame():
    return pd.DataFrame(columns=['fileName','fileSize','fileType','objectId','analysisId','studyId'])

def queryRDPC(rdpc_url,token,repo,state,objects_to_remove):
    print(rdpc_url,repo,state)
    headers = {
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Origin': '%s',
    'authorization': 'Bearer %s' % (token),
    }
    
    variables={
        "RunsFilter":{"repository":repo,"state":state},
          "analysisPage": {"from": 0,"size": 10000}
    }

    query=\
    """
    query($analysisPage: Page,$RunsFilter: RunsFilter) {
    runs(
        filter: $RunsFilter
        page: $analysisPage
        sorts: { fieldName: startTime, order: desc }
    ) {
        content {
            runId
            state
            repository
            producedAnalyses{
              analysisId
              studyId
              files{
                objectId
                name
                size
                fileType
              }
            }
        }
    }}
    """
    
    response = requests.post(rdpc_url, json={'query': query,"variables":variables},headers=headers)

    if response.status_code!=200:
        sys.exit("Following query '%s' errored, status code: %s" % (rdpc_url,str(response.status_code)))

    if response.json()['data']['runs']['content']==None:
        return objects_to_remove
    if len(response.json()['data']['runs']['content'])==0:
        return objects_to_remove

    for run in response.json()['data']['runs']['content']:

        if run['producedAnalyses']==None:
            continue
        if len(run['producedAnalyses'])==0:
            continue   
        for analysis in run['producedAnalyses']:

            if analysis['files']==None:
                continue
            if len(analysis['files'])==0:
                continue   
            for file in analysis['files']:
                count=len(objects_to_remove)
                objects_to_remove.loc[count,"fileName"]=file["name"]
                objects_to_remove.loc[count,"fileSize"]=file["size"]
                objects_to_remove.loc[count,"fileType"]=file["fileType"]
                objects_to_remove.loc[count,"objectId"]=file["objectId"]
                objects_to_remove.loc[count,"analysisId"]=analysis['analysisId']
                objects_to_remove.loc[count,"studyId"]=analysis['studyId']
                objects_to_remove.loc[count,"repository"]=run['repository']
                objects_to_remove.loc[count,"runId"]=run['runId']
                    
    
    return objects_to_remove

if __name__ == "__main__":
    main()