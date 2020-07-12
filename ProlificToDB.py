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

'''
host = "db822598633.hosting-data.io:3306";
username   = "dbo822598633";
password   = "QmulDstl2020@";
dbname     = "db822598633";
'''

mysqlServer ='mysql+pymysql://root:@localhost:3308/dstl_exp1'
engine = create_engine(mysqlServer)


df = pd.read_csv("prolific_export.csv")
pd.set_option("display.max_colwidth", None)
df.to_sql('prolificdata',con= engine ,if_exists ='append' , index= False)




