import os
import re
import json
import requests
import numpy as np
import pandas as pd
# import urllib.request
import seaborn as sns
import matplotlib.pyplot as plt
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense
#import snowflake.connector
from sys import exit
import statsmodels.api as sm
import statsmodels.formula.api as sfa
from sklearn.model_selection import train_test_split, KFold, GridSearchCV, RandomizedSearchCV
from sklearn.metrics import accuracy_score, balanced_accuracy_score, precision_recall_fscore_support, log_loss, confusion_matrix
from sklearn.linear_model import LogisticRegression, LogisticRegressionCV
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, export_graphviz
# import xgboost as xgb
# from xgboost import plot_tree, plot_importance
from load_data_api import *
from models import *

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

######################
#     Load Sales     #
######################
sales = load_sales_data()

############################
## Model Data Preparation ##
############################
## Combine metadata and sales data
all_data, dummy_features = model_data(using_art_blocks, metadata, sales)

#####################
## Select features ##
#####################
## If sale_count is included, then model can't predict tokens with no sale history (x_val)!!!

## Hashmasks ##
hm_feature_names = ['character_pct', 'eyecolor_pct', 'item_pct', 'mask_pct', 'skincolor_pct']

## Galatic Punks ## (only price_usd avaiable!)
feature_names = ['backgrounds_pct', 'hair_pct', 'species_pct', 'suits_pct', 'jewelry_pct', 'headware_pct', 'glasses_pct']

########################################
## Model 1: Stats Logistic Regression ##
########################################

## Galactic Punks ##
## Use classifier model
GP_lr_class_usd = fit_logistic(all_data, 'Galactic Punks', feature_names, bins = 5, testSize = 0.2, classifier = True, useUSD = True)
## Galactic Punks do not have price, only price USD!

## Use regressor model
GP_lr_reg_usd = fit_logistic(all_data, 'Galactic Punks', feature_names, bins = None, testSize = 0.2, classifier = False, useUSD = True)
# Fit failed!

## Hashmasks ##
## Use classifier model
HM_lr_class_usd = fit_logistic(all_data, 'Hashmasks', hm_feature_names, bins = 5, testSize = 0.2, classifier = True, useUSD = True)
HM_lr_class     = fit_logistic(all_data, 'Hashmasks', hm_feature_names, bins = 5, testSize = 0.2, classifier = True, useUSD = False)

## Use regressor model (takes very long to run!)
HM_lr_reg_usd = fit_logistic(all_data, 'Hashmasks', hm_feature_names, bins = None, testSize = 0.2, classifier = False, useUSD = True)
HM_lr_reg     = fit_logistic(all_data, 'Hashmasks', hm_feature_names, bins = None, testSize = 0.2, classifier = False, useUSD = False) 
## Fit failed


#####################################
## Model 2: CV Logistic Regression ##
#####################################

## Galactic Punks ##
## Use classifier model
GP_cv_lr_class_usd = fit_cv_logistic(all_data, 'Galactic Punks', feature_names, bins = 5, testSize = 0.2, n_fold = 5, classifier = True, useUSD = True)

## Hashmasks ##
## Use classifier model
HM_cv_lr_class_usd = fit_cv_logistic(all_data, 'Hashmasks', hm_feature_names, bins = 5, testSize = 0.2, n_fold = 5, classifier = True, useUSD = True)
HM_cv_lr_class     = fit_cv_logistic(all_data, 'Hashmasks', hm_feature_names, bins = 5, testSize = 0.2, n_fold = 5, classifier = True, useUSD = False)

## Use regressor model 
## CV LR does not take continuous y


############################
## Model 3: Decision Tree ##
############################
## Select dummy features for DT
feature_names = dummy_features['Galactic Punks']
max_depths = range(5, 50, 5)

## Galactic Punks ##
## Use classifier model (extremely overfitting!)
GP_dt_class_usd = fit_dt(all_data, 'Galactic Punks', feature_names, max_depths, bins = 5, testSize = 0.2, tolerance = 0.05, classifier = True, useUSD = True)

## Use regressor model
GP_dt_reg_usd = fit_dt(all_data, 'Galactic Punks', feature_names, max_depths, bins = None, testSize = 0.2, tolerance = 0.05, classifier = False, useUSD = True)


## Hashmasks ##
## Use classifier model (extremely overfitting!)
HM_dt_class_usd = fit_dt(all_data, 'Hashmasks', hm_feature_names, max_depths, bins = 5, testSize = 0.2, tolerance = 0.01, classifier = True, useUSD = True)
HM_dt_class     = fit_dt(all_data, 'Hashmasks', hm_feature_names, max_depths, bins = 5, testSize = 0.2, tolerance = 0.01, classifier = True, useUSD = False)

## Use regressor model (extremely overfitting!)
HM_dt_reg_usd = fit_dt(all_data, 'Hashmasks', hm_feature_names, max_depths, bins = None, testSize = 0.2, tolerance = 0.01, classifier = False, useUSD = True)
HM_dt_reg     = fit_dt(all_data, 'Hashmasks', hm_feature_names, max_depths, bins = None, testSize = 0.2, tolerance = 0.01, classifier = False, useUSD = False)

#######################################
## Model 4: Random Forest Classifier ##
#######################################
## Galactic Punks ##
## Use classifier model (extremely overfitting!)
GP_rf_class_usd = fit_rf(all_data, 'Galactic Punks', feature_names, max_depths, bins = 5, testSize = 0.2, tolerance =0.01, classifier = True, useUSD = True)

## Use regressor model (extremely overfitting!)
GP_rf_reg_usd = fit_rf(all_data, 'Galactic Punks', feature_names, max_depths, bins = 5, testSize = 0.2, tolerance =0.01, classifier = False, useUSD = True)

## Hashmasks ##
## Use classifier model
HM_rf_class_usd = fit_rf(all_data, 'Hashmasks', hm_feature_names, max_depths, bins = 5, testSize = 0.2, tolerance =0.01, classifier = True, useUSD = True)
HM_rf_class     = fit_rf(all_data, 'Hashmasks', hm_feature_names, max_depths, bins = 5, testSize = 0.2, tolerance =0.01, classifier = True, useUSD = False)

## Use regressor model (extremely overfitting!)
HM_rf_reg_usd = fit_rf(all_data, 'Hashmasks', hm_feature_names, max_depths, bins = 5, testSize = 0.2, tolerance =0.01, classifier = False, useUSD = True)
HM_rf_reg     = fit_rf(all_data, 'Hashmasks', hm_feature_names, max_depths, bins = 5, testSize = 0.2, tolerance =0.01, classifier = False, useUSD = False)


#####################################
## Model 5: Multi-layer Perceptron ##
#####################################
## Galactic Punks ##
feature_names = ['backgrounds_pct', 'hair_pct', 'species_pct', 'suits_pct', 'jewelry_pct', 'headware_pct', 'glasses_pct']
# feature_names = dummy_features['Galactic Punks']

## Use classifier model
GP_mlp_class_usd = fit_mlp(all_data, 'Galactic Punks', feature_names, bins = 5, testSize = 0.2, classifier = True, useUSD = True)

## Use regressor model
GP_mlp_class_usd = fit_mlp(all_data, 'Galactic Punks', feature_names, bins = None, testSize = 0.2, classifier = False, useUSD = True)

## Hashmasks ##
## Use classifier model
HM_mlp_class_usd = fit_mlp(all_data, 'Hashmasks', hm_feature_names, bins = 5, testSize = 0.2, classifier = True, useUSD = True)
HM_mlp_class     = fit_mlp(all_data, 'Hashmasks', hm_feature_names, bins = 5, testSize = 0.2, classifier = True, useUSD = False)

## Use regressor model
HM_mlp_reg_usd = fit_mlp(all_data, 'Hashmasks', hm_feature_names, bins = None, testSize = 0.2, classifier = False, useUSD = True)
HM_mlp_reg     = fit_mlp(all_data, 'Hashmasks', hm_feature_names, bins = None, testSize = 0.2, classifier = False, useUSD = False)

