
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
mysqlServer = 'mysql+pymysql://root:@localhost:3308/dstl_exp1'

engine = create_engine(mysqlServer)


#completed disabled in local dev
tableTrials = pd.read_sql_query(
 #   'SELECT users_prolific_expanded.*,experiment1.*, prolificdataminimal.* from users_prolific_expanded, prolificdataminimal, experiment1 , participant_category_learning WHERE users_prolific_expanded.prolific_id =prolificdataminimal.participant_id and users_prolific_expanded.id_participant = experiment1.id_participant and participant_category_learning.id = experiment1.id_participant and participant_category_learning.trained = 1  and  experiment1.id_participant not in (56,61,99,104,125,127,133,135,142,154,156,159)'
    'SELECT users_prolific_expanded.*,experiment1.*, prolificdataminimal.* from users_prolific_expanded, prolificdataminimal, experiment1 , participant_category_learning WHERE users_prolific_expanded.prolific_id =prolificdataminimal.participant_id and users_prolific_expanded.id_participant = experiment1.id_participant and participant_category_learning.id = experiment1.id_participant and participant_category_learning.trained = 1  and  experiment1.id_participant not in (  2, 13, 29, 46,92,112,113,114,   56,61,99,104,125,127,133,135,142,154,156,159)'
    , engine)


tableTrials  = tableTrials.drop(columns=[
       'status',
       'started_datetime', 'completed_date_time', 'time_taken',
       'num_approvals', 'num_rejections', 'prolific_score',
       'reviewed_at_datetime', 'entered_code'])
print(tableTrials.columns)
tableTrials.to_csv("CompleteTrialExp1Clean.csv", index=False);
tableTrials.to_sql("CompleteTrialExp1Clean", con= engine ,if_exists ='append' , index= False)