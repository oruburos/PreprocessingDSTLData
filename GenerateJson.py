
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
#mysqlServer = 'mysql+pymysql://root:@localhost:3308/dstlProlific'
mysqlServer = 'mysql+pymysql://root:@localhost:3308/dstl'

engine = create_engine(mysqlServer)


#completed disabled in local dev
tableTrials = pd.read_sql_query(
    'SELECT users_prolific_expanded.*,experimentNA.*, prolificdata.* from users_prolific_expanded, prolificdata, experimentNA WHERE users_prolific_expandedv5.prolific_id =prolificdata.participant_id  and users_prolific_expandedv5.id_participant = experimentNA.id_participant'
    , engine)
tableTrials.to_csv("CompleteTrialExp1NAloc.csv", index=False);
tableTrials.to_sql("completetrialsexp1NAloc", con= engine ,if_exists ='append' , index= False)