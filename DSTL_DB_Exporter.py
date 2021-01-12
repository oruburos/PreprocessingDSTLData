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


'''
#mysqlServer ='mysql+pymysql://'+username+':'+password+'@'+host+'/'+dbname
mysqlServer ='mysql+pymysql://root:@localhost:3308/dstlProlific'

engine = create_engine(mysqlServer)


'''SECTION FOR PARSING CATEGORY LEARNING
START'''

tableCategoryLearning = pd.read_sql_query('SELECT * FROM participant_category_learning ', engine)
pd.set_option("display.max_colwidth", None)


arrayColumns = ["type", "category_learning_trial","condition_experiment","tick","completed_by","participant_id","starting_time_trial","finishing_time_trial",
                "reaction_time",     "category",        "category_id",        "category_selected",        "category_selected_id",  "correct"]

print ("columns category learning: " + str(len(arrayColumns)))
rows_list = []
def parseCategoryLearning( replay ):
  #  print(replay)
    categoryLearningTrials = json.loads(replay['replay'])


    print(" trials category learning " + str(len(categoryLearningTrials)))
 #   print(categoryLearningTrials)
    #print(len(categoryLearningTrials.keys()))
    for trial in categoryLearningTrials:
        info = categoryLearningTrials[trial][0]['completeTrial']
        dict = {}
        dict['id_participant'] = replay['id'];
        for column in arrayColumns:
            dict[column] = info[column];
        dict['trained']=replay['trained'];
        rows_list.append(dict)


tableCategoryLearning.apply( parseCategoryLearning , axis=1 )
resultTrialsCategoryLearning = pd.DataFrame(rows_list)

'''SECTION FOR PARSING CATEGORY LEARNING
END'''


print ("parsing category learning complete")


'''SECTION FOR PARSING PRACTICE TRIALS
START'''

tablePractice = pd.read_sql_query('SELECT * FROM participantpractice', engine)

arrayColumns2 = [
"type",
			"practice_trial",
			"condition_experiment",
			"tick",
			"completed_by",
			"participant_id",
			"starting_time_trial",
			"finishing_time_trial",
			"reaction_time",
			"iti_trial",
			"previous_category_id",
			"previous_category",
			"selected_id",
			"current_position_entity_selected",
			"selected_entity_category_id",
			"selected_entity_category",
			"changed_entity_id",
			"changed_entity_category_id",
			"changed_entity_category",
			"current_position_entity_changed",
			"entities_by_category",
			"correct"
]


print ("columns practice: " + str(len(arrayColumns2)))
rows_list_practice = []
def parsePractice( replay ):
    practiceTrials = json.loads(replay['replay'])

    print(" trials practice " + str(len(practiceTrials)) )
    for trial in practiceTrials:
        info = practiceTrials[trial][0]['complete_practice_trial']
        dict = {}
        dict['id_participant'] = replay['id'];
        for column in arrayColumns2:
            if str(column) == "current_position_entity_selected":
                pos = info[column]
                if pos == "N/A":
                    dict["current_position_entity_selected_x"]= "N/A"
                    dict["current_position_entity_selected_y"] ="N/A"
                else:

                    dict["current_position_entity_selected_x"] = pos['x']
                    dict["current_position_entity_selected_y"] = pos['y']
            elif str(column) == "current_position_entity_changed":
                pos = info[column]
                if pos == "N/A":
                    dict["current_position_entity_changed_x"]= "N/A"
                    dict["current_position_entity_changed_y"] ="N/A"
                else:

                    dict["current_position_entity_changed_x"] = pos['x']
                    dict["current_position_entity_changed_y"] = pos['y']
            elif str(column) == "entities_by_category":

                pos = info[column]
                dict["UNKNOWN_entities"] = pos['UNKNOWN']
                dict["HOSTILE_entities"] = pos['HOSTILE']
                dict["NEUTRAL_entities"] = pos['NEUTRAL']
                dict["PENDING_entities"] = pos['PENDING']
                dict["UNKNOWN_entities"] = pos['UNKNOWN']
                dict["FRIEND_entities"] = pos['FRIEND']
                dict["ASSUMED_FRIEND_entities"] = pos['ASSUMED_FRIEND']
                dict["ASSUMED_HOSTILE_entities"] = pos['ASSUMED_HOSTILE']
                dict["UNCERTAIN_FRIEND_entities"] = pos['UNCERTAIN_FRIEND']
                dict["UNCERTAIN_HOSTILE_y"] = pos['UNCERTAIN_HOSTILE']
            else:

                dict[column] = info[column];
        rows_list_practice.append(dict)


tablePractice.apply( parsePractice , axis=1 )
resultTrialsPractice = pd.DataFrame(rows_list_practice)


'''SECTION FOR PARSING PRACTICE TRIALS
END'''

print ("parsing practice complete")

'''SECTION FOR PARSING MONITORING TRIALS first round
START'''

tablePerformanceFirstRound = pd.read_sql_query('SELECT * FROM participantmonitoring', engine)
arrayColumnsPerformance = [
"type",
			"monitoring_trial",
			"condition_experiment",
			"tick",
			"completed_by",
			"participant_id",
			"starting_time_trial",
			"finishing_time_trial",
			"trial_no_change",
			"trial_description",
			"reaction_time",
			"iti_trial",
			"previous_category_id",
			"previous_category",
			"current_position_entity_selected",
			"selected_entity_id",
			"selected_entity_category_id",
			"selected_entity_category",
			"changed_entity_id",
			"changed_entity_category_id",
			"changed_entity_category",
			"current_position_entity_changed",
			"entities_by_category",
			"correct"
]


print ("columns monitoring " + str(len(arrayColumnsPerformance)))
rows_list_performance = []
def parsePerformanceFirstRound( replay ):
    performanceTrials= json.loads(replay['replay'])

    print(" trials monitoring first round :" + str(len(performanceTrials)) )
    for trial in performanceTrials:
        info = performanceTrials[trial][0]['complete_monitoring_trial']
        dict = {}
        dict['id_participant'] = replay['id'];
        dict['with_12_Entities'] = replay['first_round_12'];
        for column in arrayColumnsPerformance:
            if str(column) == "current_position_entity_selected":
                pos = info[column]
                if pos == "N/A":
                    dict["current_position_entity_selected_x"] = "N/A"
                    dict["current_position_entity_selected_y"] = "N/A"
                else:
                    dict["current_position_entity_selected_x"] = pos['x']
                    dict["current_position_entity_selected_y"] = pos['y']
            elif str(column) == "current_position_entity_changed":
                pos = info[column]
                if pos == "N/A":
                    dict["current_position_entity_changed_x"] = "N/A"
                    dict["current_position_entity_changed_y"] = "N/A"
                else:
                    dict["current_position_entity_changed_x"] = pos['x']
                    dict["current_position_entity_changed_y"] = pos['y']
            elif str(column) == "entities_by_category":

                pos = info[column]
                # {'UNKNOWN': 1, 'HOSTILE': 2, 'NEUTRAL': 1, 'PENDING': 0, 'FRIEND': 0, 'ASSUMED_FRIEND': 0,
                # 'ASSUMED_HOSTILE': 0, 'UNCERTAIN_FRIEND': 0, 'UNCERTAIN_HOSTILE': 0}
                dict["UNKNOWN_entities"] = pos['UNKNOWN']
                dict["HOSTILE_entities"] = pos['HOSTILE']
                dict["NEUTRAL_entities"] = pos['NEUTRAL']
                dict["PENDING_entities"] = pos['PENDING']
                dict["UNKNOWN_entities"] = pos['UNKNOWN']
                dict["FRIEND_entities"] = pos['FRIEND']
                dict["ASSUMED_FRIEND_entities"] = pos['ASSUMED_FRIEND']
                dict["ASSUMED_HOSTILE_entities"] = pos['ASSUMED_HOSTILE']
                dict["UNCERTAIN_FRIEND_entities"] = pos['UNCERTAIN_FRIEND']
                dict["UNCERTAIN_HOSTILE_y"] = pos['UNCERTAIN_HOSTILE']
            else:
                dict[column] = info[column];
        rows_list_performance.append(dict)


tablePerformanceFirstRound.apply( parsePerformanceFirstRound , axis=1 )
resultTrialsPerformance = pd.DataFrame(rows_list_performance)

'''SECTION FOR MONITORING  TRIALS
END'''

print ("parsing monitoring first round complete")

'''SECTION FOR PARSING MONITORING TRIALS second round
START'''

tablePerformanceSecondRound = pd.read_sql_query('SELECT * FROM participantmonitoring_secondround', engine)

rows_list_performance2ndRound = []
def parsePerformanceSecondRound( replay ):

    performance2ndRoundTrials= json.loads(replay['replay'])
    print(" trials monitoring second round " + str(len(performance2ndRoundTrials)) )
    for trial in performance2ndRoundTrials:
        info = performance2ndRoundTrials[trial][0]['complete_monitoring_trial']
        dict = {}
        dict['id_participant'] = replay['id'];
        for column in arrayColumnsPerformance:
            if str(column) == "current_position_entity_selected":

                pos = info[column]

                if pos == "N/A":
                    dict["current_position_entity_selected_x"]= "N/A"
                    dict["current_position_entity_selected_y"] ="N/A"
                else:

                    dict["current_position_entity_selected_x"] = pos['x']
                    dict["current_position_entity_selected_y"] = pos['y']
            elif str(column) == "current_position_entity_changed":

                pos = info[column]

                if pos == "N/A":
                    dict["current_position_entity_changed_x"]= "N/A"
                    dict["current_position_entity_changed_y"] ="N/A"
                else:

                    dict["current_position_entity_changed_x"] = pos['x']
                    dict["current_position_entity_changed_y"] = pos['y']
            elif str(column) == "entities_by_category":

                pos = info[column]
                #{'UNKNOWN': 1, 'HOSTILE': 2, 'NEUTRAL': 1, 'PENDING': 0, 'FRIEND': 0, 'ASSUMED_FRIEND': 0,
                 #'ASSUMED_HOSTILE': 0, 'UNCERTAIN_FRIEND': 0, 'UNCERTAIN_HOSTILE': 0}
                dict["UNKNOWN_entities"] = pos['UNKNOWN']
                dict["HOSTILE_entities"] = pos['HOSTILE']
                dict["NEUTRAL_entities"] = pos['NEUTRAL']
                dict["PENDING_entities"] = pos['PENDING']
                dict["UNKNOWN_entities"] = pos['UNKNOWN']
                dict["FRIEND_entities"] = pos['FRIEND']
                dict["ASSUMED_FRIEND_entities"] = pos['ASSUMED_FRIEND']
                dict["ASSUMED_HOSTILE_entities"] = pos['ASSUMED_HOSTILE']
                dict["UNCERTAIN_FRIEND_entities"] = pos['UNCERTAIN_FRIEND']
                dict["UNCERTAIN_HOSTILE_y"] = pos['UNCERTAIN_HOSTILE']
            else:

                dict[column] = info[column];
        rows_list_performance2ndRound.append(dict)


tablePerformanceSecondRound.apply( parsePerformanceSecondRound , axis=1 )
resultTrialsPerformance2ndRound = pd.DataFrame(rows_list_performance2ndRound)

'''SECTION FOR MONITORING PRACTICE TRIALS
END'''

print ("parsing monitoring second round complete")

writer = pd.ExcelWriter('../experiment1DSTLProlificPilot.xlsx', engine='xlsxwriter')
resultTrialsCategoryLearning.to_excel(writer, sheet_name='CategoryLearningTrials',index=False)
resultTrialsPractice.to_excel(writer, sheet_name='PracticeTrials',index=False)
resultTrialsPerformance.to_excel(writer, sheet_name='PerformanceTrialsFirstRound',index=False)
resultTrialsPerformance2ndRound.to_excel(writer, sheet_name='PerformanceTrialsSecondRound',index=False)



writer.save()
