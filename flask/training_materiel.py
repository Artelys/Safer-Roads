# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 11:32:11 2020

@author: aboutet
"""

import os
import sys
import warnings
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
import pickle
    
# Modules personnels
import ml_utils
import plot
from variables import *

# Modules Python
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Librairies d'apprentissage automatique
from sklearn import tree, ensemble, metrics, model_selection
from sklearn.preprocessing import scale, StandardScaler
from sklearn.tree import export_graphviz
from sklearn.externals.six import StringIO  
from sklearn.metrics import make_scorer
from sklearn.metrics import roc_curve, auc, roc_auc_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.linear_model import LogisticRegression
from sklearn.cluster import KMeans

def training_materiel(dataM):
    dataM = pd.read_csv("C:/Users/aboutet/Documents/flask/apprentissage_materiel.csv", sep = ';', encoding = 'utf-8-sig',decimal = ',')
    
    FEATURES_HERE_QUALI_NO_ORDER = ["PRIORITYRD","AR_AUTO","AR_BUS","AR_TAXIS","AR_CARPOOL","AR_PEDEST","AR_TRUCKS","AR_TRAFF","AR_DELIV","AR_EMERVEH","AR_MOTOR",
                                "PAVED","PRIVATE","FRONTAGE","BRIDGE","TUNNEL","RAMP","TOLLWAY","POIACCESS","CONTRACC","ROUNDABOUT","INTERINTER"]
    FEATURES_HERE_QUALI_ORDERED = ["SPEED_CAT","FUNC_CLASS"]
    FEATURES_HERE_QUANTI = ["N_SHAPEPNT"]

    
    # MISE A JOUR de la liste des variables après preprocessing
    FEATURES_HERE_QUALI_NO_ORDER = FEATURES_HERE_QUALI_NO_ORDER + [ADJ_SUP, ADJ_INF] + [CONTIENT_INTERSECTION] + [CORINE]
    FEATURES_HERE_QUANTI = FEATURES_HERE_QUANTI + [WAZE_TOTAL, WAZE_ACCIDENT, WAZE_JAM, WAZE_CLOSED, WAZE_WEATHER, WAZE_HAZARD] + ALERTES_COYOTE + [TOTAL_COYOTE] + [TRAFIC]
    
    # Gardées après séléction phase 1 - CAS SANS ALERTES TEMPORELLES
    aggregation = {TRAFIC:'mean',WAZE_TOTAL:'mean', "N_SHAPEPNT":'mean',WAZE_JAM:'mean', WAZE_HAZARD:'mean',TOTAL_COYOTE:'mean'}
    QUANTI_KEEP = list(aggregation.keys())
    QUALI_ORDERED_KEEP = ["FUNC_CLASS","SPEED_CAT"]
    QUALI_NOORDER_KEEP = [CORINE, 'PAVED', 'PRIVATE', 'PRIORITYRD']
    
    QUANTI_KEEP = QUANTI_KEEP + METEO_QUANTI
    QUALI_ORDERED_KEEP = QUALI_ORDERED_KEEP
    QUALI_NOORDER_KEEP = QUALI_NOORDER_KEEP + [INFO_JOUR,JOUR_SEMAINE, MOIS, CRENEAU]
    
    dataM[CLASSES_SIMPLES] = [0 if ac > 0 else 1 for ac in dataM[ID_ACC]]
    
    baro = barometre_accidents_materiels(dataM, QUANTI_KEEP, QUALI_NOORDER_KEEP, QUALI_ORDERED_KEEP, split = 'raffinement_speeds')
    
    baro.fit(print_results = False)
    
    importance = baro.feature_importances_(plot = False)
    path = 'Barometre_materiel.pkl'
    baro.save(path)
    return importance, path

class barometre_accidents_materiels():
    
    def __init__(self, data, quantis, qualiNoOrder, qualiOrder, split = 'speed'):
        '''
        Constructor
        '''
        
        # Constantes
        self.nTrees = 200
        self.maxDepth = 100
        self.maxLeaves = 100
        self.verbose = 1
        
        self.positive_class = 0
        
        # Constantes pour le raffinement du split
        self.speeds_change = [4,6,5,7]
        self.K = 3
        self.cols_change = [COURBURE, TRAFIC, TOTAL_COYOTE]
        self.corresp_clusters = dict()
        
        # Attributs passés
        self.quantis = quantis
        self.qualiNoOrder = qualiNoOrder
        self.qualiOrder = qualiOrder
        
        self.keys = None
        self.split = split
        if self.split == 'speed':
            self.split_function = self.split_speed
        if self.split == 'raffinement_speeds':
            self.KMeans = None
            self.split_function = self.split_raffinement
            
                    
        # Données d'entrainement: déterminent d'entrée la structure des modèles dans le constructeur
        self.data = data
        self.data[CLASSES_SIMPLES] = [0 if ac > 0 else 1 for ac in self.data[ID_ACC]]
        self.target = self.data[CLASSES_SIMPLES]
        self.datas = self.split_function(self.data, first_time = True)
        # Mise à jour des clés
        self.keys = list(self.datas.keys())        
        
        self.weights = [len(self.datas[key]) for key in self.keys]
            
        # Formattage pour l'apprentissage
        self.scaler_quanti = {key: StandardScaler(with_mean = True, with_std = True) for key in self.keys}
        self.scaler_qualiOrder = {key: StandardScaler(with_mean = True, with_std = True) for key in self.keys}
        self.trains, self.targets = self.preprocessing_learning()
        self.cols = self.ref_columns()
        
        # Modele   
        self.rf = {key: ensemble.RandomForestClassifier(class_weight = 'balanced', n_estimators = self.nTrees, max_features = None, max_depth = self.maxDepth, max_leaf_nodes = self.maxLeaves, 
                                      min_samples_leaf = 5, min_samples_split = 2, verbose  = self.verbose) for key in self.keys}
        self.importance = None
        
        
    ## Split Functions that may vary
    def split_Kmeans(self, plot = False):
        return 0
        
    def split_speed(self, data, plot = True, first_time = True):
        datas = dict()
        data = data.reset_index(drop = True)
        speeds = list(data[SPEED].unique())
        for key in speeds:
            datas[key] = data[data[SPEED]==key]
        return datas
    
    def split_raffinement(self, data, first_time = True, plot = True):
        datas = dict()
        data = data.reset_index(drop = True)
        speeds = list(data[SPEED].unique())
        for key in speeds:
            datas[key] = data[data[SPEED]==key]
            
        #  Raffinement sur les vitesses contenues dans self.speeds_change
        for oldKey in self.speeds_change:
            # Récupération ancienne data et bonnes colonnes
            data_ = datas[oldKey][[AGREGATS]+self.cols_change]
            
            # Scale et KMeans, récupération colonne Cluster dans datas[key]
            data_[self.cols_change] = scale(data_[self.cols_change])
            data_ = data_[[AGREGATS]+self.cols_change].drop_duplicates(keep = 'first')
            
            if first_time:
                # La première fois, on entraine un Kmeans
                self.KMeans = KMeans(self.K)
                self.KMeans.fit(data_[self.cols_change])
                data_['Clusters'] = self.KMeans.labels_
                self.corresp_clusters[oldKey] = data_[[AGREGATS, 'Clusters']].drop_duplicates(keep = 'first').copy()
            
            # Les fois suivantes, on attribue aux nouvelles valeurs leur cluster le plus proche
            # Attention: on ne veut pas que le merge reset l'index de notre donnée car on veut pouvoir la remettre dans l'ordre
            index = datas[oldKey].index
            datas[oldKey] = datas[oldKey].merge(self.corresp_clusters[oldKey], on = AGREGATS, how = 'left')
            datas[oldKey].index = index
            datas[oldKey]['Clusters'] = datas[oldKey]['Clusters'].astype(int)
            
            # Split sur la colonne cluster dans des keys tuple
            for cluster in datas[oldKey]['Clusters'].unique():
                newKey = (oldKey, cluster)
                datas[newKey] = datas[oldKey][datas[oldKey]['Clusters']==cluster].copy()
                
                
                
            # Plot optionnel des nuages de point
            if plot:
                print("Clustering sur la vitesse (variables centrées-réduites) : "+str(oldKey))
                data_.plot.scatter(x = COURBURE, y = TOTAL_COYOTE, c = 'Clusters', colormap='viridis')
                data_.plot.scatter(x = COURBURE, y = TRAFIC, c = 'Clusters', colormap='viridis')
                data_.plot.scatter(x = TOTAL_COYOTE, y = TRAFIC, c = 'Clusters', colormap='viridis')
                plt.show()
                
            # Suppression old key
            del datas[oldKey]
            
       
        return datas
    
    ## Learning or test data preparation phase
    def preprocessing_learning(self):
        trains = dict()
        targets = dict()
        
        for key in self.keys:
            # Choix des variables explicatives et target
            trainM = self.datas[key][self.quantis+self.qualiNoOrder+self.qualiOrder]
            YM = self.datas[key][CLASSES_SIMPLES]

            # Encoding du qualitatif
            trainM = ml_utils.preproc_features(trainM, self.quantis, self.qualiOrder, self.qualiNoOrder)

            # Centrage-Normalisation du quantitatif
            self.scaler_quanti[key].fit(trainM[self.quantis])
            self.scaler_qualiOrder[key].fit(trainM[self.qualiOrder])
            
            
            trainM[self.quantis] = self.scaler_quanti[key].transform(trainM[self.quantis])
            trainM[self.qualiOrder] = self.scaler_qualiOrder[key].transform(trainM[self.qualiOrder])          
            
            trains[key] = trainM
            targets[key] = YM
        return trains, targets
    
    def preprocessing_test(self, datas):
        trains = dict()
        
        for key in self.keys:
            # Choix des variables explicatives et target
            trainM = datas[key][self.quantis+self.qualiNoOrder+self.qualiOrder]

            # Encoding du qualitatif
            trainM = ml_utils.preproc_features(trainM, self.quantis, self.qualiOrder, self.qualiNoOrder)

            # Centrage-Normalisation du quantitatif
            trainM[self.quantis] = self.scaler_quanti[key].transform(trainM[self.quantis])
            trainM[self.qualiOrder] = self.scaler_qualiOrder[key].transform(trainM[self.qualiOrder])          
            
            trains[key] = trainM
        return trains
    
    ## Proper Learning & Learning Features
    def ref_columns(self):
        # On récupère l'union des colonnes car il se peut que certaines parties des nouvelles données à prédire ne les aient pas toutes
        cols = set()
        for key in self.keys:
            cols_ = set(self.trains[key].columns)
            cols = cols.union(cols_)
        return cols
        
    
    def fit(self, print_results = True, retourne = False):
        
        # Apprentissage sur chaque split
        for key in self.keys:
            for col in self.cols:
                if col not in self.trains[key].columns:
                    self.trains[key][col] = 0
            self.trains[key] = self.trains[key][self.cols]
            self.rf[key] = self.rf[key].fit(self.trains[key], self.targets[key])
            
        if print_results:
            if retourne:
                mat, confusion, prec,recall,giny = self.predict_with_result(None, None, learning_data = True, print_ = True, retourne = retourne)
                return mat, confusion, prec,recall,giny
            else:
                self.predict_with_result(None, None, learning_data = True, print_ = True, retourne = False)
                
                
    def feature_importances_(self, plot = True):
        # On récupère la taille de tous les sous jeux de données pour pondérer la moyenne
        weights = []
        for key in self.keys:
            weights.append(len(self.datas[key]))
        # On récupère l'union des colonnes car il se peut que certaines parties des données processées ne les aient pas toutes
        cols = set()
        for key in self.keys:
            cols_ = set(self.trains[key].columns)
            cols = cols.union(cols_)
        importances = pd.DataFrame(columns=cols)
        
        
        # On récupère les importances des RF
        for i, key in enumerate(self.keys):
            df_cols = self.trains[key].columns
            df = pd.DataFrame(columns = df_cols)
            df.loc[0] = self.rf[key].feature_importances_
            for col in cols:
                if col not in df_cols:
                    df[col] = 0
            df = df[cols]
            importances.loc[i] = df.values[0,:]
            
        # Moyenne pondérée
        cols = importances.columns
        importances['weights'] = weights
        imp = []
        for col in cols:
            imp.append((importances[col]*importances['weights']).sum()/importances['weights'].sum())
        df_imp = pd.DataFrame()
        df_imp['Variables'] = imp
        df_imp.index = cols
        df_imp = df_imp.sort_values(ascending=False, by = 'Variables')
        if plot:
            df_imp.plot.bar(figsize = (10,7))
        self.importance = df_imp
        return df_imp
    
    
    ## Test & Performance Features
    def predict(self, test, learning_data = False, probas = False):
            
        # Stockage de la prédiction dans un vecteur Y qui sera réordonné
        Y = pd.DataFrame()
        if learning_data:
            tests = self.trains
        else:
            tests = self.split_function(test, first_time = False, plot = False)
            tests = self.preprocessing_test(tests)
        for key in self.keys:
            # Ajout des modalités de variables qualitatives qui ne seraitent pas présents dans l'échantiilon de test: on le met à zéro
            for col in self.cols:
                if col not in tests[key].columns:
                    tests[key][col] = 0
            tests[key] = tests[key][self.cols]
            if probas:
                pred = self.rf[key].predict_proba(tests[key])[:,0]
            else:
                 pred = self.rf[key].predict(tests[key])
            Y = Y.append(pd.DataFrame({'pred':pred}, index = tests[key].index))
        Y = Y.sort_index()
        return Y.values
    
    def predict_with_result(self, test, y_true, learning_data = False, print_ = True, retourne = False):
        if learning_data:
            y_true = self.target
        y_pred = self.predict(test, probas = False, learning_data = learning_data)
        confusion = metrics.confusion_matrix(y_true, y_pred)
        prec = metrics.accuracy_score(y_true, y_pred)
        recall = metrics.recall_score(y_true, y_pred, pos_label = self.positive_class)
        giny = 2*metrics.roc_auc_score(y_true, y_pred)-1
        
        if print_:
            print("Matrice de confusion:")
            print(confusion)
            print("Précision:")
            print(prec)
            print("Recall:")
            print(recall)
            print('Normalized Gini:')
            print(giny)
        
        if retourne:
            return y_pred, confusion, prec, recall, giny
    
        
    
    ## Save & Load Models
    def save(self, path):
        # Open the file to save as pkl file
        file = open(path, 'wb')
        pickle.dump(self, file)
        file.close()
    
    def load(self, path):
        return 0