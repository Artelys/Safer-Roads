# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 11:30:08 2020

@author: aboutet
"""
# Import libraries
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder
from sklearn import metrics, model_selection

import matplotlib.pyplot as plt
import pandas as pd

#------------------------------------------------------------------------------#
#---------------------------Preprocessing- ------------------------------------#
#------------------------------------------------------------------------------#

def preproc_features(X_train, colsQuanti, colsQualiOrdered, colsQualiNoOrder):
    #### ATTENTION: pas de valeurs nulles possibles. Il faut trouver un moyen de les déclarer comme des zéros ou une classe à part
    train = X_train.copy()
    
    ## Colonnes quantitatives
    for col in colsQuanti:
        train[col] = train[col].astype(float)
    
    ## Colonnes qualitatives non ordonnées
    if len(colsQualiNoOrder) >0:
        encNoOrder = OneHotEncoder(drop='first')
        encNoOrder.fit(train[colsQualiNoOrder])
        quali = encNoOrder.transform(train[colsQualiNoOrder]).toarray()

        # Bien nommer les colonnes par catégories
        train.drop(colsQualiNoOrder, axis = 1, inplace = True)
        qualiCols = []
        for originalCol, catList in zip(colsQualiNoOrder, encNoOrder.categories_):
            catList = catList[1:]
            l = [originalCol + '__' + str(cat) for cat in catList]
            qualiCols = qualiCols + l
        
        # Ajout des nouvelles colonnes encodées
        for col in qualiCols:
            train[col] = 0
        train[qualiCols] = quali
    
    ## Colonnes qualitatives ordonnées
    if len(colsQualiOrdered) >0:
        encOrder = OrdinalEncoder()
        encOrder.fit(train[colsQualiOrdered])
        quali = encOrder.transform(train[colsQualiOrdered])

        # Ajout des nouvelles colonnes     
        train[colsQualiOrdered] = quali

    return train

#------------------------------------------------------------------------------#
#-------------------------------- Results- ------------------------------------#
#------------------------------------------------------------------------------#

def result_classification_model(clf, X_train, y_train, X_test = None, y_test = None, tree_importance = True, positive_class = 1):  
    
    print("----- APPRENTISSAGE:")
    print("Matrice de confusion pour l'apprentissage:")
    y_pred_train = clf.predict(X_train)
    print(metrics.confusion_matrix(y_train, y_pred_train))
    print("Précision:")
    print(metrics.accuracy_score(y_train, y_pred_train))
    print("Recall:")
    print(metrics.recall_score(y_train, y_pred_train, pos_label = positive_class))
    print('F1_Score')
    print(metrics.f1_score(y_train, y_pred_train, pos_label = positive_class))
    print('Normalized Gini:')
    print(2*metrics.roc_auc_score(y_train, y_pred_train)-1)
    print('\n')
    
    if X_test is not None and y_test is not None:
        print("----- GENERALISATION:")
        y_pred_test = clf.predict(X_test)
        print("Matrice de confusion pour la généralisation:")
        print(metrics.confusion_matrix(y_test, y_pred_test))
        print("Précision:")
        print(metrics.accuracy_score(y_test, y_pred_test))
        print("Recall:")
        print(metrics.recall_score(y_test, y_pred_test, pos_label = positive_class))
        print('F1_Score')
        print(metrics.f1_score(y_test, y_pred_test, pos_label = positive_class))
        print('Normalized Gini:')
        print(2*metrics.roc_auc_score(y_test, y_pred_test)-1)
        print('\n')
    
    if tree_importance:
        plt.figure(figsize = (15,10))
        features = pd.DataFrame({"Features":X_train.columns, "Importance":clf.feature_importances_})
        features = features.set_index('Features')
        features = features.sort_values(by = "Importance", ascending = False)
        features.plot.bar(figsize = (10,7))