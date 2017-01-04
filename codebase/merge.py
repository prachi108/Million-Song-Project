import json
from pprint import pprint
with open('json_list.txt') as f:
    json_list = f.read().splitlines()

#count=0
for i in range(0,len(json_list)):
    with open("lastfm_data.json", "w") as json_file:
       for i in range(0,len(json_list)):
            filename=json_list[i]
            json_obj=open(filename).read()
            json_obj_read=json.loads(json_obj)
            json_file.write("{}\n".format(json.dumps(json_obj_read)))

#filename=json_list[0]
#json_file=open(filename).read()
#json_inter=json.loads(json_file)

    

