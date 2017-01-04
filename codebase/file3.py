#This program counts the average value of mode confidence over every decade
import csv
import sys
from collections import OrderedDict

import matplotlib.pyplot as plt
from decimal import Decimal

countMap = {}
confidenceMap = {}
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

        if (confidenceMap.has_key(yr)):
            confidenceMap[yr] += float(row['mode_confidence'])
        else:
            confidenceMap[yr] = float(row['mode_confidence'])

for key in confidenceMap:
    avgMap[key] = float(confidenceMap[key] / countMap[key])

#del avgMap['0']

keylist = avgMap.keys()
keylist.sort()

#print(avgMap);

plt.title('Trend of song confidence over the decades')
plt.xlabel('Year')
plt.ylabel('Mode Confidence')
plt.plot(range(len(avgMap)), avgMap.values())
plt.xticks(range(len(avgMap)), keylist)

plt.show()