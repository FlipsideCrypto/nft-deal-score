

import os
import re
import json
import requests
import numpy as np
import pandas as pd
import urllib.request
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

###################################
## Train, Test, Validation split ##
###################################
## Define x, y and split train, test set

def model_data_split(all_data, collection_name, feature_names, classifier, bins, testSize = 0.2, useUSD = True):
    all_data = all_data[collection_name]

    if classifier: ## y is categorical
        if useUSD :
            ## Use price n.a. as validation set, the rest as train & test set
            model_data = all_data[all_data['price_usd_median'].notna()]
            val_data = all_data[all_data['price_usd_median'].isna()]
            
            ## Transform y USD price into categorical
            model_data['price_group'] = pd.qcut(model_data['price_usd_median'], q = bins)
            model_data['price_label'] = pd.qcut(model_data['price_usd_median'], q = bins, labels = np.array(range(0, bins)))
        else:
            if 'price_median' in all_data:    
                ## Use price n.a. as validation set, the rest as train & test set
                model_data = all_data[all_data['price_median'].notna()]
                val_data = all_data[all_data['price_median'].isna()]
                
                ## Transform y price into categorical
                model_data['price_group'] = pd.qcut(model_data['price_median'], q=5)
                model_data['price_label'] = pd.qcut(model_data['price_median'], q=5, labels=[0, 1, 2, 3, 4])
            else: ## No price avaiable
                print('No price available!')
                exit()
            
        model_data['price_label'] = model_data['price_label'].astype('int64')
        model_data['price_group'] = model_data['price_group'].astype('category')
        
        # mapping = model_data.groupby(['price_label','price_group']).size().reset_index().rename(columns={0:'count'})
        mapping = model_data[['price_label','price_group']].drop_duplicates()
       
        y = model_data[['price_label']]
        x = model_data.drop(['price_label'], axis = 1)
        x_val = val_data[feature_names]
        x_train_all, x_test_all, y_train, y_test = train_test_split(x, y, test_size = testSize, random_state = 123)
        
    else: ## y is numeric continuous dependent variable
        mapping = None
        print('Not a classifier model, so no class names!')
        if useUSD:
            ## Use price n.a. as validation set, the rest as train & test set
            model_data = all_data[all_data['price_usd_median'].notna()]
            val_data = all_data[all_data['price_usd_median'].isna()]
            
            y = model_data[['price_usd_median']]
            x = model_data.drop(['price_usd_median'], axis = 1)
            x_val = val_data[feature_names]

        else:
            if 'price_median' in all_data:  
                ## Use price n.a. as validation set, the rest as train & test set
                model_data = all_data[all_data['price_median'].notna()]
                val_data = all_data[all_data['price_median'].isna()]
                
                y = model_data[['price_median']]
                x = model_data.drop(['price_median'], axis = 1)
                x_val = val_data[feature_names]
            else:
                print('No price available!')
                exit()

        x_train_all, x_test_all, y_train, y_test = train_test_split(x, y, test_size = testSize, random_state = 123)
    
    x_train = x_train_all[feature_names]
    x_test = x_test_all[feature_names]

    train = pd.concat([y_train, x_train], axis = 1)
    test = pd.concat([y_test, x_test], axis = 1)
    

    return train, test, x_train, x_test, y_train, y_test, x_val, mapping

###############################
## Stats Logistic Regression ##
###############################
## mnlogit doesn't take categorical features, need to use dummy or WOE for features
def fit_logistic(all_data, collection_name, feature_names, bins, testSize, classifier = True, useUSD = True):
    
    ## Split data
    train, test, x_train, x_test, y_train, y_test, x_val, class_name = model_data_split(all_data, collection_name, feature_names, classifier, bins, testSize, useUSD)
    
    ## Run model
    lr_stat = sm.MNLogit(y_train, x_train, axis=1).fit()
    print(lr_stat.summary())
    
    lr_pred_train = lr_stat.predict(x_train).idxmax(axis=1)
    lr_pred_test = lr_stat.predict(x_test).idxmax(axis=1)
    lr_pred_val = lr_stat.predict(x_val).idxmax(axis=1)
    
      
    ## Performance
    if lr_pred_train.isnull().all():
        prediction =  print('Fit failed, all n.a. predictions!')
        
    else:
        print('The balanced accuracy for Logistic Regression training set is: {:.3f}'.format(balanced_accuracy_score(y_train, lr_pred_train)))
        print('The balanced accuracy for Logistic Regression testing set is: {:.3f}'.format(balanced_accuracy_score(y_test, lr_pred_test)))
        
        ## Create prediction dataframe for train, test, val
        pred = pd.concat([pd.DataFrame(lr_pred_train), pd.DataFrame(lr_pred_test), pd.DataFrame(lr_pred_val)], axis = 0)   
        pred = pred.rename(columns = {0:"price_label"})
        pred = pred.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
        pred = pred.rename(columns = {"price_label":"predict_price_label", "price_group":"predict_price_group"})
        
        actual = pd.concat([y_train, y_test], axis = 0)
        actual = actual.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
    
        prediction = pred.reset_index().merge(actual, on='token_id', how='left').set_index('token_id')

        if class_name is not None: ## for classifier model plot confusion matrix
            lr_cm = confusion_matrix(y_train['price_label'], lr_pred_train)
            lr_cm_value = lr_cm/lr_cm.sum(axis = 1)[:, np.newaxis]
            names = class_name.iloc[:,0].astype('str') + '-' + class_name.iloc[:,1].astype('str')
            plot_confusion_matrix(lr_cm_value*100, classes = names, title = 'Confusion Matrix (%) - Logistic Regression')
        
          
    return prediction

############################
## CV Logistic Regression ##
############################

def fit_cv_logistic(all_data, collection_name, feature_names, bins, testSize, n_fold, classifier = True, useUSD = True):
    ## Split data
    train, test, x_train, x_test, y_train, y_test, x_val, class_name = model_data_split(all_data, collection_name, feature_names, classifier, bins, testSize, useUSD)

    # Create C as a geometric progression series 
    C_list = [round(1e-3*10**j,5) for j in range(6)]

    # Define the split into n folds 
    kf = KFold(n_splits=n_fold, random_state=123, shuffle=True)

    # Run cross-validation using all C values above
    lr_cv = LogisticRegressionCV(solver="lbfgs", Cs=C_list, random_state=123, cv = kf.split(x_train)).fit(x_train, y_train)

    lr_cv_train = lr_cv.predict(x_train)
    lr_cv_test = lr_cv.predict(x_test)
    lr_cv_val = lr_cv.predict(x_val)

    lr_cv_probs_train = lr_cv.predict_proba(x_train)
    lr_cv_probs_test = lr_cv.predict_proba(x_test)
    # lr_cv_probs_val = lr_cv.predict_proba(x_val)
    
        ## Performance
    if np.isnan(sum(lr_cv_train)):
        prediction = print('Fit failed, all n.a. predictions!')
    else:
        print('The balanced accuracy for CV Logistic Regression training set is: {:.3f}'.format(balanced_accuracy_score(y_train, lr_cv_train)))
        print('The balanced accuracy for CV Logistic Regression testing set is: {:.3f}'.format(balanced_accuracy_score(y_test, lr_cv_test)))

        print('The log loss for CV Logistic Regression training set is: {:.3f}'.format(log_loss(y_train, lr_cv_probs_train)))
        print('The log loss for CV Logistic Regression testing set is: {:.3f}'.format(log_loss(y_test, lr_cv_probs_test)))
        
        ## Create prediction dataframe for train, test, val
        lr_cv_train = pd.DataFrame(lr_cv_train).set_index(x_train.index)
        lr_cv_test = pd.DataFrame(lr_cv_test).set_index(x_test.index)
        lr_cv_val = pd.DataFrame(lr_cv_val).set_index(x_val.index)
        
        pred = pd.concat([pd.DataFrame(lr_cv_train), pd.DataFrame(lr_cv_test), pd.DataFrame(lr_cv_val)], axis = 0)   
        pred = pred.rename(columns = {0:"price_label"})
        pred = pred.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
        pred = pred.rename(columns = {"price_label":"predict_price_label", "price_group":"predict_price_group"})
        
        actual = pd.concat([y_train, y_test], axis = 0)
        actual = actual.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
    
        prediction = pred.reset_index().merge(actual, on='token_id', how='left').set_index('token_id')
        
        if class_name is not None:  ## for classifier model plot confusion matrix
            
            lr_cv_cm = confusion_matrix(y_train['price_label'], lr_cv_train)
            lr_cv_cm_value = lr_cv_cm/lr_cv_cm.sum(axis = 1)[:, np.newaxis]
            names = class_name.iloc[:,0].astype('str') + '-' + class_name.iloc[:,1].astype('str')
            plot_confusion_matrix(lr_cv_cm_value*100, classes = names, title = 'Confusion Matrix (%) - CV Logistic Regression')
        
    
    return prediction


###################
## Decision Tree ##
###################

def fit_dt(all_data, collection_name, feature_names, max_depths, bins, testSize, tolerance , classifier = True, useUSD = True):
    
    ## Split data
    train, test, x_train, x_test, y_train, y_test, x_val, class_name = model_data_split(all_data, collection_name, feature_names, classifier, bins, testSize, useUSD)

    ## Trial Model  (hyperparameters to add: min_ssample_leaf, n_estimators, min_samples_split)
    if classifier:  ## Use classifier model
        dt_table = pd.DataFrame(columns=['max_depth', 'mean_accuracy', 'accuracy'])
        for d in max_depths:
          dt_trial = DecisionTreeClassifier(random_state = 123, max_depth = d).fit(x_train, y_train)
          y_pred = dt_trial.predict(x_train)
          rsq = dt_trial.score(x_train, y_train)
          accuracy = balanced_accuracy_score(y_train, pd.DataFrame(y_pred))
          dt_table = dt_table.append({'max_depth':d,
                                      'mean_accuracy':rsq,
                                      'accuracy': accuracy}, ignore_index = True)
          
          ## Final optimal tree depth
          plt.plot(dt_table['max_depth'], dt_table['accuracy'])
          plt.xlabel('max tree depth')
          plt.ylabel('Accuracy')
          plt.title('Trials of different tree depth vs. Accuracy')

    
    else: ## Use regressor model
        dt_table = pd.DataFrame(columns=['max_depth', 'mean_accuracy'])
        for d in max_depths:
          dt_trial = DecisionTreeRegressor(random_state = 123, max_depth = d).fit(x_train, y_train)
          y_pred = dt_trial.predict(x_train)
          rsq = dt_trial.score(x_train, y_train)
          dt_table = dt_table.append({'max_depth':d,
                                      'mean_accuracy':rsq}, ignore_index = True)


    print(dt_table) 
    plt.plot(dt_table['max_depth'], dt_table['mean_accuracy'])
    plt.xlabel('max tree depth')
    plt.ylabel('mean_accuracy')
    plt.title('Trials of different tree depth vs. mean_accuracy')
              

    ## Print optimal tree depth
    for i in range(len(max_depths)-1):
        print(i)
        if (dt_table.loc[i+1,'mean_accuracy'] - dt_table.loc[i,'mean_accuracy']) < tolerance:
            opt_depth = dt_table.loc[i+1,'max_depth']
            print("Optimal tree depth is: {:.3f}".format(opt_depth))
            break
        else:
            opt_depth = None
            print("Tolerance level not reached!")
        
    ## Final Model
    if opt_depth is not None:
        if classifier: ## Use classifier model
            dt = DecisionTreeClassifier(random_state = 123, max_depth = opt_depth).fit(x_train, y_train)
            y_pred_train = dt.predict(x_train)
            y_pred_test = dt.predict(x_test)
            y_pred_val = dt.predict(x_val)
            
            print('The balanced accuracy for Decision Tree training set is: {:.3f}'.format(balanced_accuracy_score(y_train, y_pred_train)))
            print('The balanced accuracy for Decision Tree testing set is: {:.3f}'.format(balanced_accuracy_score(y_test, y_pred_test)))
            
            ## Create prediction dataframe for train, test, val
            y_pred_train = pd.DataFrame(y_pred_train).set_index(x_train.index)
            y_pred_test = pd.DataFrame(y_pred_test).set_index(x_test.index)
            y_pred_val = pd.DataFrame(y_pred_val).set_index(x_val.index)
            
            pred = pd.concat([pd.DataFrame(y_pred_train), pd.DataFrame(y_pred_test), pd.DataFrame(y_pred_val)], axis = 0)   
            pred = pred.rename(columns = {0:"price_label"})
            pred = pred.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
            pred = pred.rename(columns = {"price_label":"predict_price_label", "price_group":"predict_price_group"})
            
            actual = pd.concat([y_train, y_test], axis = 0)
            actual = actual.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
            
            prediction = pred.reset_index().merge(actual, on='token_id', how='left').set_index('token_id')
            
            if class_name is not None:  ## for classifier model plot confusion matrix
                    
                    cm = confusion_matrix(y_train['price_label'], round(pd.DataFrame(y_pred_train),0))
                    cm_value = cm/cm.sum(axis = 1)[:, np.newaxis]
                    names = class_name.iloc[:,0].astype('str') + '-' + class_name.iloc[:,1].astype('str')
                    plot_confusion_matrix(cm_value*100, classes = names, title = 'Confusion Matrix (%) - Decistion Tree Classifier')


        else: ## Use regressor model
            dt = DecisionTreeRegressor(random_state = 123, max_depth = opt_depth).fit(x_train, y_train)
            y_pred_train = dt.predict(x_train)
            y_pred_test = dt.predict(x_test)
            y_pred_val = dt.predict(x_val)
            
            ## Create prediction dataframe for train, test, val
            y_pred_train = pd.DataFrame(y_pred_train).set_index(x_train.index)
            y_pred_test = pd.DataFrame(y_pred_test).set_index(x_test.index)
            y_pred_val = pd.DataFrame(y_pred_val).set_index(x_val.index)
            
            pred = pd.concat([pd.DataFrame(y_pred_train), pd.DataFrame(y_pred_test), pd.DataFrame(y_pred_val)], axis = 0)   
            pred = pred.rename(columns = {pred.columns[0]:"price_label_predicted"})
            
            actual = pd.concat([y_train, y_test], axis = 0)
            actual = actual.rename(columns = {actual.columns[0]:"price_label_actual"})
            
            prediction = pred.reset_index().merge(actual, on='token_id', how='left').set_index('token_id')
    
        print('The mean_accuracy for Decision Tree training set is: {:.3f}'.format(dt.score(x_train, y_train)))
        print('The mean_accuracy for Decision Tree testing set is: {:.3f}'.format(dt.score(x_test, y_test)))
        
    
    else:
        prediction = print("Optimal tree depth is not found! Increase tolerance level.")
                     
    return prediction

###################
## Random Forest ##
###################

def fit_rf(all_data, collection_name, feature_names, max_depths, bins, testSize, tolerance , classifier = True, useUSD = True):
    
    ## Split data
    train, test, x_train, x_test, y_train, y_test, x_val, class_name = model_data_split(all_data, collection_name, feature_names, classifier, bins, testSize, useUSD)

    ## Trial Model  (hyperparameters to add: min_ssample_leaf, n_estimators, min_samples_split)
    if classifier:  ## Use classifier model
        rf_table = pd.DataFrame(columns=['max_depth', 'mean_accuracy', 'accuracy'])
        for d in max_depths:
          rf_trial = RandomForestClassifier(random_state = 123, 
                                            max_depth = d, 
                                            criterion = 'entropy',
                                            class_weight = 'balanced_subsample').fit(x_train, y_train)
          y_pred = rf_trial.predict(x_train)
          rsq = rf_trial.score(x_train, y_train)
          accuracy = balanced_accuracy_score(y_train, pd.DataFrame(y_pred))
          rf_table = rf_table.append({'max_depth':d,
                                      'mean_accuracy':rsq,
                                      'accuracy': accuracy}, ignore_index = True)
          
          ## Final optimal tree depth
          plt.plot(rf_table['max_depth'], rf_table['accuracy'])
          plt.xlabel('max tree depth')
          plt.ylabel('Accuracy')
          plt.title('Trials of different tree depth vs. Accuracy')

    
    else: ## Use regressor model
        rf_table = pd.DataFrame(columns=['max_depth', 'R-squared'])
        for d in max_depths:
          rf_trial = RandomForestRegressor(random_state = 123,
                                           max_depth = d,
                                           criterion = 'squared_error').fit(x_train, y_train)
          y_pred = rf_trial.predict(x_train)
          rsq = rf_trial.score(x_train, y_train)
          rf_table = rf_table.append({'max_depth':d,
                                      'mean_accuracy':rsq}, ignore_index = True)


    print(rf_table) 
    plt.plot(rf_table['max_depth'], rf_table['mean_accuracy'])
    plt.xlabel('max tree depth')
    plt.ylabel('mean_accuracy')
    plt.title('Trials of different tree depth vs. mean_accuracy')
              

    ## Print optimal tree depth
    for i in range(len(max_depths)-1):
        print(i)
        if (rf_table.loc[i+1,'mean_accuracy'] - rf_table.loc[i,'mean_accuracy']) < tolerance:
            opt_depth = rf_table.loc[i+1,'max_depth']
            print("Optimal tree depth is: {:.3f}".format(opt_depth))
            break
        else:
            opt_depth = None
            print("Tolerance level not reached!")
    
    
    
    ## Final Model
    if opt_depth is not None:
        if classifier:  ## Use classifier model
            rf = RandomForestClassifier(random_state=123, 
                                        max_depth = opt_depth, 
                                        criterion = 'entropy',
                                        class_weight = 'balanced_subsample').fit(x_train, y_train)
            y_pred_train = rf.predict(x_train)
            y_pred_test = rf.predict(x_test)
            y_pred_val = rf.predict(x_val)
            
            print('The balanced accuracy for Random Forest training set is: {:.3f}'.format(balanced_accuracy_score(y_train, y_pred_train)))
            print('The balanced accuracy for Random Forest testing set is: {:.3f}'.format(balanced_accuracy_score(y_test, y_pred_test)))
    
            ## Create prediction dataframe for train, test, val
            y_pred_train = pd.DataFrame(y_pred_train).set_index(x_train.index)
            y_pred_test = pd.DataFrame(y_pred_test).set_index(x_test.index)
            y_pred_val = pd.DataFrame(y_pred_val).set_index(x_val.index)
            
            pred = pd.concat([pd.DataFrame(y_pred_train), pd.DataFrame(y_pred_test), pd.DataFrame(y_pred_val)], axis = 0)   
            pred = pred.rename(columns = {0:"price_label"})
            pred = pred.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
            pred = pred.rename(columns = {"price_label":"predict_price_label", "price_group":"predict_price_group"})
                
            actual = pd.concat([y_train, y_test], axis = 0)
            actual = actual.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
                
            prediction = pred.reset_index().merge(actual, on='token_id', how='left').set_index('token_id')
            
            if class_name is not None:  ## for classifier model plot confusion matrix
            
                cm = confusion_matrix(y_train['price_label'], round(pd.DataFrame(y_pred_train),0))
                cm_value = cm/cm.sum(axis = 1)[:, np.newaxis]
                names = class_name.iloc[:,0].astype('str') + '-' + class_name.iloc[:,1].astype('str')
                plot_confusion_matrix(cm_value*100, classes = names, title = 'Confusion Matrix (%) - Random Forest Classifier')
                
        else: ## Use regressor model
            rf = RandomForestRegressor(random_state = 123, 
                                       max_depth = opt_depth,
                                       criterion = 'squared_error').fit(x_train, y_train)
            y_pred_train = rf.predict(x_train)
            y_pred_test = rf.predict(x_test)
            y_pred_val = rf.predict(x_val)
            
            ## Create prediction dataframe for train, test, val
            y_pred_train = pd.DataFrame(y_pred_train).set_index(x_train.index)
            y_pred_test = pd.DataFrame(y_pred_test).set_index(x_test.index)
            y_pred_val = pd.DataFrame(y_pred_val).set_index(x_val.index)
            
            pred = pd.concat([pd.DataFrame(y_pred_train), pd.DataFrame(y_pred_test), pd.DataFrame(y_pred_val)], axis = 0)   
            pred = pred.rename(columns = {pred.columns[0]:"price_label_predicted"})
            
            actual = pd.concat([y_train, y_test], axis = 0)
            actual = actual.rename(columns = {actual.columns[0]:"price_label_actual"})
            
            prediction = pred.reset_index().merge(actual, on='token_id', how='left').set_index('token_id')
    
        print('The mean accuracy for Decision Tree training set is: {:.3f}'.format(rf.score(x_train, y_train)))
        print('The mean accuracy for Decision Tree testing set is: {:.3f}'.format(rf.score(x_test, y_test)))
        
    
    else:
        prediction = print("Optimal tree depth is not found! Increase tolerance level.")
                  
    return prediction


############################
## Multi Layer Perceptron ##
############################

def fit_mlp(all_data, collection_name, feature_names, bins, testSize, classifier = True, useUSD = True):
    
    ## Split data
    train, test, x_train, x_test, y_train, y_test, x_val, class_name = model_data_split(all_data, collection_name, feature_names, classifier, bins, testSize, useUSD)
    
    n_features = x_train.shape[1]

    model = Sequential()
    model.add(Dense(10, activation='relu', kernel_initializer='he_normal', input_shape=(n_features,)))
    model.add(Dense(8, activation='relu', kernel_initializer='he_normal'))
    
    if classifier: ## Use classifier
        model.add(Dense(bins, activation='softmax'))
        model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])
        model.fit(x_train, y_train, epochs=150, batch_size=32, verbose=0)
    
        loss_train, accuracy_train = model.evaluate(x_train, y_train, verbose=0)
        loss_test, accuracy_test = model.evaluate(x_test, y_test, verbose=0)
        print('The accuracy for MLP training set is:: %.3f' % accuracy_train)
        print('The accuracy for MLP testing set is:: %.3f' % accuracy_test)
        
        print('The log loss for MLP training set is:: %.3f' % loss_train)
        print('The log loss for MLP testing set is:: %.3f' % loss_test)
        
        y_pred_train = np.argmax(model.predict(x_train), axis = 1)
        y_pred_test  = np.argmax(model.predict(x_test), axis = 1)
        y_pred_val   = np.argmax(model.predict(x_val), axis = 1)
        
        ## Create prediction dataframe for train, test, val
        y_pred_train = pd.DataFrame(y_pred_train).set_index(x_train.index)
        y_pred_test = pd.DataFrame(y_pred_test).set_index(x_test.index)
        y_pred_val = pd.DataFrame(y_pred_val).set_index(x_val.index)
        
        pred = pd.concat([pd.DataFrame(y_pred_train), pd.DataFrame(y_pred_test), pd.DataFrame(y_pred_val)], axis = 0)   
        pred = pred.rename(columns = {0:"price_label"})
        pred = pred.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
        pred = pred.rename(columns = {"price_label":"predict_price_label", "price_group":"predict_price_group"})
        
        actual = pd.concat([y_train, y_test], axis = 0)
        actual = actual.reset_index().merge(class_name, on='price_label', how='left').set_index('token_id')
        
        prediction = pred.reset_index().merge(actual, on='token_id', how='left').set_index('token_id')
        
        if class_name is not None:  ## for classifier model plot confusion matrix
        
            cm = confusion_matrix(y_train['price_label'], round(pd.DataFrame(y_pred_train),0))
            cm_value = cm/cm.sum(axis = 1)[:, np.newaxis]
            names = class_name.iloc[:,0].astype('str') + '-' + class_name.iloc[:,1].astype('str')
            plot_confusion_matrix(cm_value*100, classes = names, title = 'Confusion Matrix (%) - Decistion Tree Classifier')


    else: ## Use regressor
        model.add(Dense(1))
        model.compile(optimizer='adam', loss='mse')
        model.fit(x_train, y_train, epochs=150, batch_size=32, verbose=0)
    
        mse_train = model.evaluate(x_train, y_train, verbose=0)
        mse_test  = model.evaluate(x_test, y_test, verbose=0)
        
        print('The RMSE for MLP training set is:: %.3f' % np.sqrt(mse_train))
        print('The RMSE for MLP testing set is:: %.3f' % np.sqrt(mse_test))
        
        y_pred_train = model.predict(x_train)
        y_pred_test  = model.predict(x_test)
        y_pred_val   = model.predict(x_val)
            
        ## Create prediction dataframe for train, test, val
        y_pred_train = pd.DataFrame(y_pred_train).set_index(x_train.index)
        y_pred_test = pd.DataFrame(y_pred_test).set_index(x_test.index)
        y_pred_val = pd.DataFrame(y_pred_val).set_index(x_val.index)
            
        pred = pd.concat([pd.DataFrame(y_pred_train), pd.DataFrame(y_pred_test), pd.DataFrame(y_pred_val)], axis = 0)   
        pred = pred.rename(columns = {pred.columns[0]:"price_label_predicted"})
            
        actual = pd.concat([y_train, y_test], axis = 0)
        actual = actual.rename(columns = {actual.columns[0]:"price_label_actual"})
            
        prediction = pred.reset_index().merge(actual, on='token_id', how='left').set_index('token_id')
    
                     
    return prediction
