import os
import json
from jsonmerge import merge
from pprint import pprint
rootdir = 'C:\Users\Sharang Bhat\Documents\Big Data Analytics\Project\lastfm_subset'
count=0
json_list=[]
for subdir,dirs,files in os.walk(rootdir):
    for file in files:
        filename=os.path.join(subdir,file)
        json_data=open(filename).read()
        json_name=filename.split('\\')
        json_name=json_name[len(json_name)-1]
        json_list.append(json_name)
        with open(json_name,'w') as outfile:
            json.dump(json_data,outfile)
json_listfile=open("json_list.txt","w")
for item in json_list:
    json_listfile.write("%s\n" % item)
