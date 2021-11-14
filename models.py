#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 21:19:13 2021

@author: elainehu
"""

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

###########################
## Plot Confusion Matrix ##
###########################
def plot_confusion_matrix(cm, classes=None, perc = True, title = 'my title'):
    """Plots a confusion matrix."""
    if classes is not None:
        ax = sns.heatmap(cm, xticklabels=classes, yticklabels=classes, vmin=0., vmax=1., annot=True, cmap='Greens')
        bottom, top = ax.get_ylim()
        ax.set_ylim(bottom+0.5, top-0.5)
        for t in ax.texts: t.set_text(t.get_text()+"%")
        plt.xlabel('Predicted')
        plt.ylabel('Actual')
        plt.title(title)


    else:
        ax = sns.heatmap(cm, vmin=0., vmax=1., cmap = 'Greens')
        bottom, top = ax.get_ylim()
        ax.set_ylim(bottom+0.5, top-0.5)
        for t in ax.texts: t.set_text(t.get_text()+"%")
        plt.xlabel('Predicted')
        plt.ylabel('Actual')
        plt.title(title)

###########################
## Train, Test Val split ##
###########################
## Define x, y and split train, test set
def model_data_split(model_data, feature_names, model_type = 'classifier'):
    if model_type == 'classifier':
        ## Transform y price into categorical
        model_data['price_group'] = pd.qcut(model_data['price_median'], q=5)
        model_data['price_label'] = pd.qcut(model_data['price_median'], q=5, labels=[0, 1, 2, 3, 4])
        model_data['price_label'] = model_data['price_label'].astype('int64')
        model_data['price_group'].value_counts(dropna=False)

        y = model_data[['price_label']]
        x = model_data
        x_train_all, x_test_all, y_train, y_test = train_test_split(x, y, test_size = 0.20, random_state = 123)
        
    else: ## type is numeric continuous dependent variable
        y = model_data[['price_median']]
        x = model_data
        x_train_all, x_test_all, y_train, y_test = train_test_split(x, y, test_size = 0.20, random_state = 123)
    
    x_train = x_train_all[feature_names]
    x_test = x_test_all[feature_names]

    train = pd.concat([y_train, x_train], axis = 1)
    test = pd.concat([y_test, x_test], axis = 1)

    return train, test, x_train, x_test, y_train, y_test
