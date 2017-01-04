#This program count the average value of tempo over every decade
import csv
import sys
from collections import OrderedDict

import matplotlib.pyplot as plt
from decimal import Decimal

countMap = {}
tempoMap = {}
avgMap = {}

csv.field_size_limit(sys.maxsize)
with open('test.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if int(row['year']) < 1960:
            continue
        yr = (int(row['year'])/10)*10
        if (countMap.has_key(yr)):
            countMap[yr] += int(1)
        else:
            countMap[yr] = int(1)

        if (tempoMap.has_key(yr)):
            tempoMap[yr] += float(row['tempo'])
        else:
            tempoMap[yr] = float(row['tempo'])

for key in tempoMap:
    avgMap[key] = float(tempoMap[key] / countMap[key])

#del avgMap['0']

keylist = avgMap.keys()
keylist.sort()

#print(avgMap);

plt.title('Trend of song tempo over the decades')
plt.xlabel('Year')
plt.ylabel('Tempo (in BPM)')
plt.plot(range(len(avgMap)), avgMap.values())
plt.xticks(range(len(avgMap)), keylist)

plt.show()