
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import json

'''
host = "localhost:3308";
username   = "root";
password   = "";
dbname     = "dstl";
mysqlServer ='mysql+pymysql://root:@localhost:3308/dstl'
'''

# mysqlServer ='mysql+pymysql://'+username+':'+password+'@'+host+'/'+dbname
mysqlServer = 'mysql+pymysql://root:@localhost:3308/dstlProlific'

engine = create_engine(mysqlServer)

tableTrials = pd.read_sql_query(
    'SELECT users_prolific_expandedv5.*,expanded.*,prolificdata.* FROM participant_category_learning as categorylearning,   users_prolific_expandedv5, prolificdata,participantpractice as practice,	participantmonitoring_secondround as monitoring1, 	participantmonitoring as monitoring2, 	experiment as expanded where 	users_prolific_expandedv5.prolific_id = prolificdata.participant_id	and  	users_prolific_expandedv5.id_participant = categorylearning.id  	and 	users_prolific_expandedv5.id_participant = practice.id 	and	users_prolific_expandedv5.id_participant =  monitoring1.id  	and users_prolific_expandedv5.id_participant = monitoring2.id	and 	prolificdata.status = "APPROVED" and users_prolific_expandedv5.completed = 1'
    , engine)
tableTrials.to_csv("CompleteTrial.csv", index=False);
tableTrials.to_sql("completetrials", con= engine ,if_exists ='append' , index= False)