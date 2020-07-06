
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
    'SELECT users_prolific_expandedv5.*,experiment.*, prolificdata.* from users_prolific_expandedv5, prolificdata, experiment WHERE users_prolific_expandedv5.prolific_id =prolificdata.participant_id and users_prolific_expandedv5.completed = 1 and users_prolific_expandedv5.id_participant = experiment.id_participant'
    , engine)
tableTrials.to_csv("CompleteTrialExp1.csv", index=False);
tableTrials.to_sql("completetrialsexp1", con= engine ,if_exists ='append' , index= False)