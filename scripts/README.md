python return_SongObjects.py -p all -u https://song.rdpc-qa.cancercollaboratory.org
python return_failedAnalysisObjects.py -s https://song.rdpc-qa.cancercollaboratory.org -g https://api.rdpc-qa.cancercollaboratory.org/graphql -t ${ego_token}
python come_up_with_a_better_name.py -t ${token} -u https://song.rdpc-qa.cancercollaboratory.org -c ../outputs/example-qa.csv
