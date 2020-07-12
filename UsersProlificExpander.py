import pandas as pd
from sqlalchemy import create_engine
import json
import ValuesExperiment

#mysqlServer ='mysql+pymysql://root:@localhost:3308/dstlProlific'
mysqlServer ='mysql+pymysql://root:@localhost:3308/dstl_exp1'
engine = create_engine(mysqlServer)

#usersprolific = pd.read_sql_query('SELECT * FROM usersprolific where completed = 1 ', engine)
usersprolific = pd.read_sql_query('SELECT * FROM usersprolific', engine)

pd.set_option("display.max_colwidth", None)

#special cases
arrayColumns = ["colours", "category_colour_association","shortMonitoringTask","longMonitoringTask"]
#usersprolific[['category_colour', 'order_trials_12','order_trials_24']] .apply( parseCategoryLearning , axis=1 )
def categories_color_relationship( cats):
    categories = json.loads(cats)
   # print(ValuesExperiment.Categories.keys())
    trialsAcostado = []
    for key in ValuesExperiment.Categories:
        colorForCategory = categories.get(key)
        if colorForCategory:
            trialsAcostado.append(colorForCategory)
        else:
            trialsAcostado.append('N/A')

    return pd.Series(trialsAcostado)


def concatenateTrials( categories):
    cats = json.loads(categories)
    print("original")
    print(cats)


    resilt = " => ".join(cats)

    print("after")
    print(resilt)

    return resilt


keysv2 = list(ValuesExperiment.Categories.keys() )
usersprolific[ keysv2] = usersprolific['category_colour_association'].apply( categories_color_relationship)

usersprolific['order_trials_12'] = usersprolific['shortMonitoringTask'].apply( concatenateTrials)
usersprolific['order_trials_24'] = usersprolific['longMonitoringTask'].apply( concatenateTrials)
usersprolific = usersprolific.drop(['session_id','category_colour_association','colours', 'longMonitoringTask' ,'shortMonitoringTask'  ,  'PENDING' ], axis=1)
usersprolific.to_sql('users_prolific_expanded',con= engine ,if_exists ='append' , index= False)

print(usersprolific )