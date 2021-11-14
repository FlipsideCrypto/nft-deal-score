import os
import re
import json
import requests
import numpy as np
import pandas as pd
import urllib.request
import seaborn as sns
import matplotlib.pyplot as plt
#import tensorflow as tf
#import snowflake.connector
import statsmodels.api as sm
import statsmodels.formula.api as sfa
from sklearn.model_selection import train_test_split, KFold, GridSearchCV, RandomizedSearchCV
from sklearn.metrics import accuracy_score, balanced_accuracy_score, precision_recall_fscore_support, log_loss, confusion_matrix
from sklearn.linear_model import LogisticRegression, LogisticRegressionCV
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, export_graphviz
from load_data_api import *
from models import *

os.chdir('/Users/elainehu/Dropbox/Flipside/NFT_model/nft-deal-score')

#########################
# Select NFT Collection #
#########################

CONTRACT_ADDRESSES = [ '0xc2c747e0f7004f9e8817db2ca4997657a7746928' ]
contract_address_query = "'{}'".format( "','".join(CONTRACT_ADDRESSES) )
using_art_blocks = len(set(['0x059edd72cd353df5106d2b9cc5ab83a52287ac3a','0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270']).intersection(set(CONTRACT_ADDRESSES))) > 0


#########################
#     Load Metadata     #
#########################
metadata = load_metadata()
## ** better to write this function with an input argument - contract address **

######################
#     Load Sales     #
######################
sales = load_sales_data()
## ** better to write this function with an input argument - contract address **

############################
## Model Data Preparation ##
############################
## Combine metadata and sales data
all_data = model_data(using_art_blocks, metadata, sales, collection = 'Hashmasks')

## Use price n.a. as validation set, the rest as train & test set
model_data = all_data[all_data['price_median'].notna()]
val_data = all_data[all_data['price_median'].isna()]

## Select relevant features  
feature_names = ['character_pct', 'eyecolor_pct', 'item_pct', 'mask_pct', 'skincolor_pct', 'sale_count']
train, test, x_train, x_test, y_train, y_test = model_data_split(model_data, feature_names, model_type = 'classifier')

## Create validation set as tokens with no sale price
x_val = val_data[feature_names]
    
########################################
## Model 1: Stats Logistic Regression ##
########################################
## mnlogit doesn't take categorical, need to use dummy or WOE
#lr_stat = sfa.mnlogit("price_label ~ character_pct + eyecolor_pct + item_pct + mask_pct + skincolor_pct", train).fit()
lr_stat = sm.MNLogit(train['price_label'], train.drop(['price_label'], axis=1)).fit()
lr_stat.params
lr_stat.summary()

lr_pred_train = lr_stat.predict(x_train).idxmax(axis=1)
lr_pred_test = lr_stat.predict(x_test).idxmax(axis=1)
lr_pred_val = lr_stat.predict(x_val).idxmax(axis=1)

## Performance
print(balanced_accuracy_score(y_train, lr_pred_train))
print(balanced_accuracy_score(y_test, lr_pred_test))

lr_cm = confusion_matrix(y_train['price_label'], lr_pred_train)
lr_cm_value = lr_cm/lr_cm.sum(axis = 1)[:, np.newaxis]
class_name = np.unique(model_data['price_label'].astype(str) +  '-' + model_data['price_group'].astype(str))
plot_confusion_matrix(lr_cm_value*100, classes = class_name, title = 'Confusion Matrix (%) - Logistic Regression')

#####################################
## Model 2: CV Logistic Regression ##
#####################################

# Create C as a geometric progression series 
C_list = [round(1e-3*10**j,5) for j in range(6)]

# Define the split into 5 folds 
kf = KFold(n_splits=5, random_state=0, shuffle=True)

# Run cross-validation using all C values above
lr_cv = LogisticRegressionCV(solver="lbfgs", Cs=C_list, random_state=123, cv = kf.split(x_train)).fit(x_train, y_train)

lr_cv_train = lr_cv.predict(x_train)
lr_cv_test = lr_cv.predict(x_test)

lr_cv_probs_train = lr_cv.predict_proba(x_train)
lr_cv_probs_test = lr_cv.predict_proba(x_test)

## Performance
print(balanced_accuracy_score(y_train, lr_cv_train))
print(balanced_accuracy_score(y_test, lr_cv_test))

print(log_loss(y_train, lr_cv_probs_train))
print(log_loss(y_test, lr_cv_probs_test))

lr_cv_cm = confusion_matrix(y_train['price_label'], lr_cv_train)
lr_cv_cm_value = lr_cv_cm/lr_cv_cm.sum(axis = 1)[:, np.newaxis]
plot_confusion_matrix(lr_cv_cm_value*100, classes = class_name, title = 'Confusion Matrix (%) - Logistic Regression CV')

############################
## Model 3: Decision Tree ##
############################

x_ct_names = ['character_Female', 'character_Golden Robot',
       'character_Male', 'character_Mystical', 'character_Puppet',
       'character_Robot', 'eyecolor_Blue', 'eyecolor_Dark', 'eyecolor_Freak',
       'eyecolor_Glass', 'eyecolor_Green', 'eyecolor_Heterochromatic',
       'eyecolor_Mystical', 'eyecolor_Painted', 'item_Book', 'item_Bottle',
       'item_Golden Toilet Paper', 'item_Mirror', 'item_No Item',
       'item_Shadow Monkey', 'item_Toilet Paper', 'mask_Abstract',
       'mask_African', 'mask_Animal', 'mask_Aztec', 'mask_Basic',
       'mask_Chinese', 'mask_Crayon', 'mask_Doodle', 'mask_Hawaiian',
       'mask_Indian', 'mask_Mexican', 'mask_Pixel', 'mask_Steampunk',
       'mask_Street', 'mask_Unique', 'mask_Unmasked', 'skincolor_Blue',
       'skincolor_Dark', 'skincolor_Freak', 'skincolor_Gold', 'skincolor_Gray',
       'skincolor_Light', 'skincolor_Mystical', 'skincolor_Steel',
       'skincolor_Transparent', 'skincolor_Wood']

x_train_ct = x_train_all[x_ct_names]
x_test_ct = x_test_all[x_ct_names]

max_depths = [5,10,15,20,25,30,35,40]
dt_table = pd.DataFrame(columns=['max_depth', 'R-squared', 'accuracy'])

for d in max_depths:
  dt_trial = DecisionTreeRegressor(random_state = 123, max_depth = d).fit(x_train_ct, y_train)
  y_pred = dt_trial.predict(x_train_ct)
  rsq = dt_trial.score(x_train_ct, y_train)
  accuracy = balanced_accuracy_score(y_train, round(pd.DataFrame(y_pred),0))
  dt_table = dt_table.append({'max_depth':d,
                              'R-squared':rsq,
                              'accuracy': accuracy}, ignore_index = True)

plt.plot(dt_table['max_depth'], dt_table['R-squared'])
plt.xlabel('max tree depth')
plt.ylabel('R-squared')
plt.title('Trials of different tree depth vs. R-squared')

plt.plot(dt_table['max_depth'], dt_table['accuracy'])
plt.xlabel('max tree depth')
plt.ylabel('Accuracy')
plt.title('Trials of different tree depth vs. Accuracy')

## Print optimal tree depth
for i in range(6):
    if (dt_table.loc[i+1,'accuracy'] - dt_table.loc[i,'accuracy'])<0.01:
        opt_depth = dt_table.loc[i+1,'max_depth']
        print("Optimal tree depth is: {:.3f}".format(opt_depth))
        break

## Final Model
dt = DecisionTreeRegressor(random_state = 123, max_depth = opt_depth).fit(x_train_ct, y_train)
y_pred_train = dt.predict(x_train_ct)
y_pred_test = dt.predict(x_test_ct)

print(balanced_accuracy_score(y_train, round(pd.DataFrame(y_pred_train),0)))
print(balanced_accuracy_score(y_test, round(pd.DataFrame(y_pred_test),0)))

print(dt.score(x_train_ct, y_train))
print(dt.score(x_test_ct, y_test))

cm = confusion_matrix(y_train['price_label'], round(pd.DataFrame(y_pred_train),0))
cm_value = lr_cv_cm/lr_cv_cm.sum(axis = 1)[:, np.newaxis]
plot_confusion_matrix(cm_value*100, classes = class_name, title = 'Confusion Matrix (%) - Decistion Tree')

#######################################
## Model 4: Random Forest Classifier ##
#######################################

# Train random forest classifier 
rf = RandomForestClassifier(random_state=123, criterion='entropy').fit(x_train_ct, y_train)
rf_y = rf.predict(x_train_ct)
rf_y_p = rf.predict_proba(x_train_ct)

rf_y_test = rf.predict(x_test_ct)
rf_y_p_test = rf.predict_proba(x_test_ct)

print('The balanced accuracy for Random Forest training set is: {:.3f}'.format(balanced_accuracy_score(y_train, rf_y)))
print('The balanced accuracy for Random Forest testing set is: {:.3f}'.format(balanced_accuracy_score(y_test, rf_y_test)))
print(dt.score(x_train_ct, y_train))
print(dt.score(x_test_ct, y_test))

print('The log loss for Random Forest training set is: {:.3f}'.format(log_loss(y_train, rf_y_p)))
print('The log loss for Random Forest testing set is: {:.3f}'.format(log_loss(y_test, rf_y_p_test)))



######################################
## Model 5: Random Forest Regressor ##
######################################

# Train random forest classifier 
rf = RandomForestRegressor(random_state=123, criterion='entropy').fit(x_train, y_train)
rf_y = rf.predict(x_train_ct)
rf_y_p = rf.predict_proba(x_train_ct)

rf_y_test = rf.predict(x_test_ct)
rf_y_p_test = rf.predict_proba(x_test_ct)

print('The balanced accuracy for Random Forest training set is: {:.3f}'.format(balanced_accuracy_score(y_train, rf_y)))
print('The balanced accuracy for Random Forest testing set is: {:.3f}'.format(balanced_accuracy_score(y_test, rf_y_test)))
print(dt.score(x_train_ct, y_train))
print(dt.score(x_test_ct, y_test))

print('The log loss for Random Forest training set is: {:.3f}'.format(log_loss(y_train, rf_y_p)))
print('The log loss for Random Forest testing set is: {:.3f}'.format(log_loss(y_test, rf_y_p_test)))

