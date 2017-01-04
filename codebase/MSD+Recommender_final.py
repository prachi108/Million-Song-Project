
# coding: utf-8

# In[38]:

from pprint import pprint
from collections import defaultdict
import itertools
import sys
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS,MatrixFactorizationModel


sc = SparkContext(appName="Recommender")



def to_string(line):
    line=line.split("\t")
    return (str(line[0]),str(line[1]),str(line[2]))

def float_tostring(line):
    return (str(line[0]),str(line[1]),str(line[2]))

def take5(tup_list):
    tup_top5=tup_list[:5]
    return tup_top5

def sort_tup_list(list_tup):
    list_tup.sort(key=lambda x: -x[1])
    return list_tup


music_data=sc.textFile("C:\Users\Sharang Bhat\Documents\Big Data Analytics\Project\data_msd.txt").take(200000)
music_data=sc.parallelize(music_data)
ratings=music_data.map(to_string)
userstoint = ratings.map(lambda (a,b,c): a).distinct().zipWithIndex()
reversemappingofusers = userstoint.map(lambda (a,b) : (b,a))
ratingswithuniqueuserid = ratings.map(lambda (a,b,c) : (a,(b,c))).join(userstoint)
newratings = ratingswithuniqueuserid.map(lambda  (userid,((songid, count), usertoint)) : (usertoint, songid, count))
songstoint = ratings.map(lambda (usertoint, songid, count): songid).distinct().zipWithIndex()
ratingswithuniquesongid = newratings.map(lambda (usertoint,songid,count) : (songid,(usertoint,count))).join(songstoint)
newratings = ratingswithuniquesongid.map(lambda  (songid,((usertoint, count), songtoint)) : (usertoint, songtoint, count))
reversemappingofsongs = songstoint.map(lambda (a,b) : (b,a))
newratings=newratings.map(float_tostring)
newratings=newratings.filter(lambda (a,b,c): c.isdigit())


# ## Recommender



training_RDD, validation_RDD, test_RDD = newratings.randomSplit([6, 2, 2], seed=0L)
validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

seed = 5L
iterations = 10
regularization_parameter = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
tolerance = 0.02

min_error = float('inf')
best_rank = -1
best_iteration = -1




for rank in ranks:
    model = ALS.train(training_RDD, rank, seed=seed, iterations=iterations,
                      lambda_=regularization_parameter)
    predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
    error = sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    errors[err] = error
    err += 1
    print 'For rank %s the RMSE is %s' % (rank, error)
    if error < min_error:
        min_error = error
        best_rank = rank



print 'The best model was trained with rank %s' % best_rank




model = ALS.train(test_RDD, best_rank, seed=seed, iterations=iterations,
                      lambda_=regularization_parameter)
predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
error = sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

print 'For testing data the RMSE is %s' % (error)


predictions_users=predictions.map(lambda ((a,b),c): (a,(b,c)) )
prediction_users=predictions_users.join(reversemappingofusers)
predictions_users=prediction_users.map(lambda (a,((b,c),d)): (d,(b,c)))
predictions_users = predictions_users.map(lambda (a,(b,c)): (b,(a,c)))
predictions_users=predictions_users.join(reversemappingofsongs)
predictions_users=predictions_users.map(lambda (sn,((u,c),sid)): (u,(sid,c)))
predictions_users=predictions_users.groupByKey()
predictions_users_test=predictions_users.map(lambda line: (line[0],list(line[1])))
predictions_users_test=predictions_users_test.map(lambda line: (line[0],sort_tup_list(line[1])))
predictions_users_take5 = predictions_users_test.map(lambda (a,b):(b,a))
predictions_top5=predictions_users_take5.map(lambda line: (take5(line[0]),line[1]))
predictions_users=predictions_top5.map(lambda (a,b): (b,a))
predictions_users.take(10)
predictions_users_final=predictions_users.collect()
