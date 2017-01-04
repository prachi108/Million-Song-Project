# This program counts the number of songs corresponding to each year
import csv

import sys

import matplotlib.pyplot as plt

countMap = {}
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

#print(countMap);

keylist = countMap.keys()
keylist.sort()
plt.title('Number of songs released over the decades')
plt.xlabel('Year')
plt.ylabel('Number of songs')
plt.bar(range(len(countMap)), countMap.values(), align='center')
plt.xticks(range(len(countMap)), keylist)

plt.show()


