import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import json
from DSTL import ValuesExperiment

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



'''SECTION FOR USERSPROLIFIC'''
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
            trialsAcostado.append('NA')

    return pd.Series(trialsAcostado)


def concatenateTrials( categories):
    cats = json.loads(categories)
    resilt = " => ".join(cats)
    return resilt


keysv2 = list(ValuesExperiment.Categories.keys() )
usersprolific[ keysv2] = usersprolific['category_colour_association'].apply( categories_color_relationship)

usersprolific['order_trials_12'] = usersprolific['shortMonitoringTask'].apply( concatenateTrials)
usersprolific['order_trials_24'] = usersprolific['longMonitoringTask'].apply( concatenateTrials)
usersprolificexpanded = usersprolific.drop(['session_id','category_colour_association','colours', 'longMonitoringTask' ,'shortMonitoringTask'  ,  'PENDING' ], axis=1)
usersprolificexpanded.to_sql('users_prolific_expandedv5',con= engine ,if_exists ='append' , index= False)
'''END USERSPROLIFIC'''


'''SECTION FOR PARSING CATEGORY LEARNING
START'''

tableCategoryLearning = pd.read_sql_query('SELECT * FROM participant_category_learning ', engine)
pd.set_option("display.max_colwidth", None)

arrayColumns = ["type", "category_learning_trial", "condition_experiment", "tick", "completed_by", "participant_id",
                 "starting_time_trial", "finishing_time_trial",
                 "reaction_time", "category", "category_id", "category_selected", "category_selected_id", "correct"]

print("columns category learning: " + str(len(arrayColumns)))
rows_list = []

def parseCategoryLearning(replay):
    #  print(replay)
    categoryLearningTrials = json.loads(replay['replay'])

   # print(" trials category learning " + str(len(categoryLearningTrials)))
   # print(categoryLearningTrials)
    # print(len(categoryLearningTrials.keys()))
    for trial in categoryLearningTrials:
        info = categoryLearningTrials[trial][0]['complete_category_learning_trial']
        dict = {}
        dict['id_participant'] = replay['id'];
        for column in arrayColumns:
            if str(info[column]) == "completeTrial":
                #print("Sutituyendo complete trial")
                dict[column] = "complete_category_learning_trial";
            else:
                if info[column]=='N/A':
                    dict[column] = 'NA';
                else:
                    dict[column] = info[column];

        dict['trained'] = replay['trained'];
        rows_list.append(dict)

#
tableCategoryLearning.apply(parseCategoryLearning, axis=1)
resultTrialsCategoryLearning = pd.DataFrame(rows_list)

pd.set_option("display.max_colwidth", None)
resultTrialsCategoryLearning.to_sql('category_learning_trials_expanded',con= engine ,if_exists ='append' , index= False)

''' SECTION FOR PARSING CATEGORY LEARNING
END
'''

print("parsing category learning complete")

'''SECTION FOR PARSING PRACTICE TRIALS
START'''

tablePractice = pd.read_sql_query('SELECT * FROM participantpractice', engine)

arrayColumns2 = [
    "type",
    "practice_trial",#complete_practice_trial
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

print("columns practice: " + str(len(arrayColumns2)))
rows_list_practice = []


def parsePractice(replay):
    practiceTrials = json.loads(replay['replay'])

    print(" trials practice " + str(len(practiceTrials)))

    print(" trials practice " + str(practiceTrials))
    for trial in practiceTrials:
        info = practiceTrials[trial][0]['complete_practice_trial']
        dict = {}
        dict['id_participant'] = replay['id'];
        for column in arrayColumns2:
            if str(column) == "current_position_entity_selected":
                pos = info[column]
                if pos == "N/A":
                    dict["current_position_entity_selected_x"] = "NA"
                    dict["current_position_entity_selected_y"] = "NA"
                else:

                    dict["current_position_entity_selected_x"] = pos['x']
                    dict["current_position_entity_selected_y"] = pos['y']
            elif str(column) == "current_position_entity_changed":
                pos = info[column]
                if pos == "N/A":
                    dict["current_position_entity_changed_x"] = "NA"
                    dict["current_position_entity_changed_y"] = "NA"
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
                if info[column]=='N/A':
                    dict[column] = 'NA';
                else:
                    dict[column] = info[column];

        rows_list_practice.append(dict)


tablePractice.apply(parsePractice, axis=1)
resultTrialsPractice = pd.DataFrame(rows_list_practice)


pd.set_option("display.max_colwidth", None)
resultTrialsPractice.to_sql('practice_trials_expanded',con= engine ,if_exists ='append' , index= False)


'''SECTION FOR PARSING PRACTICE TRIALS
END'''

print("parsing practice complete")

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

print("columns monitoring " + str(len(arrayColumnsPerformance)))
rows_list_performance = []


def parsePerformanceFirstRound(replay):
    performanceTrials = json.loads(replay['replay'])
    #print(" trials monitoring first round :" + str(len(performanceTrials)))
    for trial in performanceTrials:
        info = performanceTrials[trial][0]['complete_monitoring_trial']
        dict = {}
        dict['id_participant'] = replay['id'];
        dict['with_12_Entities'] = replay['first_round_12'];
        for column in arrayColumnsPerformance:
            if str(column) == "current_position_entity_selected":
                pos = info[column]
                if pos == "N/A":
                    dict["current_position_entity_selected_x"] = "NA"
                    dict["current_position_entity_selected_y"] = "NA"
                else:
                    dict["current_position_entity_selected_x"] = pos['x']
                    dict["current_position_entity_selected_y"] = pos['y']
            elif str(column) == "current_position_entity_changed":
                pos = info[column]
                if pos == "N/A":
                    dict["current_position_entity_changed_x"] = "NA"
                    dict["current_position_entity_changed_y"] = "NA"
                else:
                    dict["current_position_entity_changed_x"] = pos['x']
                    dict["current_position_entity_changed_y"] = pos['y']
            elif str(column) == "trial_description":
                change = info[column]
               # print(change)
               # print(change[0])
                dict["from_category_id"] = change[0]
                dict["to_category_id"] = change[1]

            elif str(column) == "entities_by_category":

                pos = info[column]
                # {'UNKNOWN': 1, 'HOSTILE': 2, 'NEUTRAL': 1, 'PENDING': 0, 'FRIEND': 0, 'ASSUMED_FRIEND': 0,
                # 'ASSUMED_HOSTILE': 0, 'UNCERTAIN_FRIEND': 0, 'UNCERTAIN_HOSTILE': 0}
                dict['UNKNOWN_entities'] = pos['UNKNOWN']
                dict['HOSTILE_entities'] = pos['HOSTILE']
                dict['NEUTRAL_entities'] = pos['NEUTRAL']
                dict['PENDING_entities'] = pos['PENDING']
                dict['UNKNOWN_entities'] = pos['UNKNOWN']
                dict['FRIEND_entities'] = pos['FRIEND']
                dict['ASSUMED_FRIEND_entities'] = pos['ASSUMED_FRIEND']
                dict['ASSUMED_HOSTILE_entities'] = pos['ASSUMED_HOSTILE']
                dict['UNCERTAIN_FRIEND_entities'] = pos['UNCERTAIN_FRIEND']
                dict['UNCERTAIN_HOSTILE_y'] = pos['UNCERTAIN_HOSTILE']
            else:
                if info[column] == 'N/A':
                    dict[column] = 'NA';
                else:
                    dict[column] = info[column];
        rows_list_performance.append(dict)


tablePerformanceFirstRound.apply(parsePerformanceFirstRound, axis=1)
resultTrialsPerformance = pd.DataFrame(rows_list_performance)


#print(resultTrialsPerformance.head(2))
resultTrialsPerformance.to_sql('monitoring_1st_round_trials_expanded',con= engine ,if_exists ='append' , index= False)

'''SECTION FOR MONITORING  TRIALS
END'''

print("parsing monitoring first round complete")

'''SECTION FOR PARSING MONITORING TRIALS second round
START'''

tablePerformanceSecondRound = pd.read_sql_query('SELECT * FROM participantmonitoring_secondround', engine)
rows_list_performance2ndRound = []


def parsePerformanceSecondRound(replay):
    performance2ndRoundTrials = json.loads(replay['replay'])
    print(" trials monitoring second round " + str(len(performance2ndRoundTrials)))
    for trial in performance2ndRoundTrials:
        info = performance2ndRoundTrials[trial][0]['complete_monitoring_trial']
        dict = {}
        dict['id_participant'] = replay['id'];
        for column in arrayColumnsPerformance:
            if str(column) == "current_position_entity_selected":

                pos = info[column]

                if pos == "N/A":
                    dict["current_position_entity_selected_x"] = "NA"
                    dict["current_position_entity_selected_y"] = "NA"
                else:

                    dict["current_position_entity_selected_x"] = pos['x']
                    dict["current_position_entity_selected_y"] = pos['y']
            elif str(column) == "current_position_entity_changed":

                pos = info[column]

                if pos == "N/A":
                    dict["current_position_entity_changed_x"] = "NA"
                    dict["current_position_entity_changed_y"] = "NA"
                else:

                    dict["current_position_entity_changed_x"] = pos['x']
                    dict["current_position_entity_changed_y"] = pos['y']
            elif str(column) == "trial_description":
                change = info[column]
                # print(change)
                # print(change[0])
                dict["from_category_id"] = change[0]
                dict["to_category_id"] = change[1]

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

                if info[column] == 'N/A':
                    dict[column] = 'NA';
                else:
                    dict[column] = info[column];
        rows_list_performance2ndRound.append(dict)


tablePerformanceSecondRound.apply(parsePerformanceSecondRound, axis=1)
resultTrialsPerformance2ndRound = pd.DataFrame(rows_list_performance2ndRound)


resultTrialsPerformance2ndRound.to_sql('monitoring_2nd_round_trials_expanded',con= engine ,if_exists ='append' , index= False)
'''SECTION FOR MONITORING PRACTICE TRIALS
END'''

print("parsing monitoring second round complete")

#writer = pd.ExcelWriter('experiment1DSTLProlificPilot.xlsx', engine='xlsxwriter')
#resultTrialsCategoryLearning.to_excel(writer, sheet_name='CategoryLearningTrials', index=False)
#resultTrialsPractice.to_excel(writer, sheet_name='PracticeTrials', index=False)
#resultTrialsPerformance.to_excel(writer, sheet_name='PerformanceTrialsFirstRound', index=False)
#resultTrialsPerformance2ndRound.to_excel(writer, sheet_name='PerformanceTrialsSecondRound', index=False)

#resultTrialsPractice.to_csv("Prueba.csv", index=False);


#writer.save()


trials = [ resultTrialsCategoryLearning , resultTrialsPractice , resultTrialsPerformance,resultTrialsPerformance2ndRound ];


total = pd.concat(trials);




total.to_sql('experimentNA',con= engine ,if_exists ='append' , index= False)

#total.to_csv("CompleteTrialsNA.csv", index=False);



#tableTrials = pd.read_sql_query('SELECT * FROM participant_category_learning as categorylearning, usersprolific, prolificdata, participantpractice as practice, participantmonitoring_secondround as monitoring1, participantmonitoring as monitoring2, experiment as expanded where usersprolific.prolific_id = prolificdata.participant_id and usersprolific.id_participant = categorylearning.id and usersprolific.id_participant = practice.id and usersprolific.id_participant = monitoring1.id and usersprolific.id_participant = monitoring2.id and prolificdata.status = "APPROVED"', engine)
#tableTrials.to_csv("CompleteTrials.csv", index=False);