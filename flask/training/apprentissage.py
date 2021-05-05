# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 14:51:39 2020

@author: aboutet
"""

# Librairies d'apprentissage automatique
from sklearn import tree, ensemble, metrics, model_selection
from sklearn.preprocessing import scale, StandardScaler
from sklearn.tree import export_graphviz
from sklearn.externals.six import StringIO  
from sklearn.metrics import make_scorer
from sklearn.metrics import roc_curve, auc, roc_auc_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.model_selection import ShuffleSplit

from sklearn.linear_model import LogisticRegression
from sklearn.cluster import KMeans
import pickle

def training_corporel(dataC)
    # Colonnes input Waze
    TYPE = 'TYPE'
    SUBTYPE = 'SUBTYPE'
    
    DATE = 'Date'
    
    # Colonnes output Waze
    ID_WAZE = "id_waze"
    WAZE_TOTAL = 'Total alertes Waze'
    WAZE_ACCIDENT = "Nombre alertes Waze ACCIDENT"
    WAZE_JAM = "Nombre alertes Waze BOUCHONS"
    WAZE_CLOSED = "Nombre alertes Waze ROUTE BARREE"
    WAZE_WEATHER = "Nombre alertes Waze DANGERMETEO"
    WAZE_HAZARD = "Nombre alertes Waze DANGERROUTE"
    
    # Colonnes Coyote
    ID_COYOTE = "id_coyote"
    ALERTES_COYOTE = ['Nombre alertes coyote Retrecissement', 'Nombre alertes coyote Véhicule arrêté', 'Nombre alertes coyote Visibilité réduite', 'Nombre alertes coyote Chaussée dégradée', 
                'Nombre alertes coyote Accident', 'Nombre alertes coyote Route glissante']
    TOTAL_COYOTE = "Nombre total d'alertes Coyote"
    
    ALERTES = [WAZE_TOTAL, WAZE_ACCIDENT, WAZE_JAM, WAZE_CLOSED, WAZE_WEATHER, WAZE_HAZARD] + ALERTES_COYOTE + [TOTAL_COYOTE]
    
    # Colonnes HERE
    ID_SEGT = 'LINK_ID'
    CONTIENT_INTERSECTION = "Contient intersection"
    
    FEATURES_HERE_QUALI_NO_ORDER = ["PRIORITYRD","AR_AUTO","AR_BUS","AR_TAXIS","AR_CARPOOL","AR_PEDEST","AR_TRUCKS","AR_TRAFF","AR_DELIV","AR_EMERVEH","AR_MOTOR",
                                "PAVED","PRIVATE","FRONTAGE","BRIDGE","TUNNEL","RAMP","TOLLWAY","POIACCESS","CONTRACC","ROUNDABOUT","INTERINTER"]
    FEATURES_HERE_QUALI_ORDERED = ["SPEED_CAT","FUNC_CLASS"]
    FEATURES_HERE_QUANTI = ["N_SHAPEPNT"]
    
    NODES_1 = 'REF_IN_ID'
    NODES_2 = 'NREF_IN_ID'
    ADJ_SUP = "Vitesse adjacente supérieure"
    ADJ_INF = "Vitesse adjacente inférieure"
    
    TRAFIC = 'Trafic redressé'
    
    # Corine Land Cover
    CORINE = 'CORINE'
    
    
    # ACCIDENTS
    
    # Colonnes BAAC
    ID_BAAC = 'ID_BAAC'
    
    
    # Materiel
    ID_ASSU = 'id_materiel'
    ID_SDIS = 'id_sdis'
    
    # Tous accidents
    ID_ACC = 'ID_ACCIDENT'
    TYPE = 'TYPE'
    
    # Outputs
    NB_ACC = 'NOMBRE D ACCIDENTS'
    NB_ACC_MAT = 'NOMBRE D ACCIDENTS MATERIELS'
    NB_ACC_CORPO = 'NOMBRE D ACCIDENTS CORPORELS'
    CLASSES_SIMPLES = 'Classes simples'
    CLASSES_DOUBLES = 'Classes doubles'
    CLASSES_CORPOREL = "Classe Accident Corporel"
    CLASSES_MATERIEL = "Classe Accident Matériel"
    
    # Temporel
    CRENEAU = "Creneau_Horaire"
    JOUR_MOIS = "Jour du mois"
    JOUR_SEMAINE = "Jour de la semaine"
    MOIS = "Mois"
    ANNEE = "Annee"
    SAISON = "Saison"
    
    TIMESTEP = [CRENEAU, JOUR_MOIS,MOIS, ANNEE]
    INFO_JOUR = 'Information jour'
    
    # Météo
    METEO_QUANTI = ['Temperature', 'Pression 0m', 'Direction Vent', 'Vitesse Vent', 
                    'Pluviometrie 1h', 'Pluviometrie 6h']
    AGREGATS = 'Agregat'
    SPEED = 'SPEED_CAT'
    COURBURE = 'N_SHAPEPNT'
    
    aggregation = {"Trafic redressé":'mean',"Total alertes Waze":'mean', "N_SHAPEPNT":'mean',"Nombre alertes Waze BOUCHONS":'mean', "Nombre alertes Waze DANGERROUTE":'mean',"Nombre total d'alertes Coyote":'mean'}
    QUANTI_KEEP = list(aggregation.keys())
    QUALI_ORDERED_KEEP = ["FUNC_CLASS","SPEED_CAT"]
    QUALI_NOORDER_KEEP = ["Contient intersection",'PAVED','Vitesse adjacente supérieure']
    
    QUANTI_KEEP = QUANTI_KEEP + METEO_QUANTI
    QUALI_ORDERED_KEEP = QUALI_ORDERED_KEEP
    QUALI_NOORDER_KEEP = QUALI_NOORDER_KEEP + [INFO_JOUR,JOUR_SEMAINE, MOIS, CRENEAU]
    
    # MISE A JOUR de la liste des variables après preprocessing
    FEATURES_HERE_QUALI_NO_ORDER = FEATURES_HERE_QUALI_NO_ORDER + [ADJ_SUP, ADJ_INF] + [CONTIENT_INTERSECTION] + [CORINE]
    FEATURES_HERE_QUANTI = FEATURES_HERE_QUANTI + [WAZE_TOTAL, WAZE_ACCIDENT, WAZE_JAM, WAZE_CLOSED, WAZE_WEATHER, WAZE_HAZARD] + ALERTES_COYOTE + [TOTAL_COYOTE] + [TRAFIC]
    
    class barometre_accidents_corporels():
        
        def __init__(self, data, quantis, qualiNoOrder, qualiOrder):
            '''
            Constructor
            '''
            
            # Constantes
            self.nTrees = 200
            self.maxDepth = 30
            self.maxLeaves = 30
            self.verbose = 5
            
            self.positive_class = 0
            
            # Attributs passés
            self.quantis = quantis
            self.qualiNoOrder = qualiNoOrder
            self.qualiOrder = qualiOrder
                
                        
            # Données d'entrainement: déterminent d'entrée la structure des modèles dans le constructeur
            self.data = data
            self.data[CLASSES_SIMPLES] = [0 if ac > 0 else 1 for ac in self.data[ID_ACC]]
                
            # Formattage pour l'apprentissage
            self.scaler_quanti = StandardScaler(with_mean = True, with_std = True)
            self.scaler_qualiOrder = StandardScaler(with_mean = True, with_std = True)
            self.train, self.target = self.preprocessing_learning()
            
            # Récupération de la liste des variables (colonnes) qu'il faut impérativement donner en entrée de l'algorithme
            self.cols = self.train.columns
            
            # Modele   
            self.rf = ensemble.RandomForestClassifier(class_weight = 'balanced', n_estimators = self.nTrees, max_features = None, max_depth = self.maxDepth, max_leaf_nodes = self.maxLeaves, 
                                          min_samples_leaf = 5, min_samples_split = 2, verbose  = self.verbose)
            self.importance = None
            
            
    
        
        ## Learning or test data preparation phase
        def preprocessing_learning(self):
            trains = dict()
            targets = dict()
            
            # Choix des variables explicatives et target
            train = self.data[self.quantis+self.qualiNoOrder+self.qualiOrder]
            target = self.data[CLASSES_SIMPLES]
    
            # Encoding du qualitatif
            train = ml_utils.preproc_features(train, self.quantis, self.qualiOrder, self.qualiNoOrder)
    
            # Centrage-Normalisation du quantitatif
            #train[self.quantis] = scale(train[self.quantis])
            #train[self.qualiOrder] = scale(train[self.qualiOrder])          
            
            # Scaler should scale the same way all data it will see in the future
            self.scaler_quanti.fit(train[self.quantis])
            self.scaler_qualiOrder.fit(train[self.qualiOrder])
            train[self.quantis] = self.scaler_quanti.transform(train[self.quantis])
            train[self.qualiOrder] = self.scaler_qualiOrder.transform(train[self.qualiOrder])   
            
            return train, target
        
        
        def preprocessing_test(self, data):
            trains = dict()
            targets = dict()
            
            # Choix des variables explicatives et target
            train = data[self.quantis+self.qualiNoOrder+self.qualiOrder]
    
            # Encoding du qualitatif
            train = ml_utils.preproc_features(train, self.quantis, self.qualiOrder, self.qualiNoOrder)
    
            # Centrage-Normalisation du quantitatif
            train[self.quantis] = self.scaler_quanti.transform(train[self.quantis])
            train[self.qualiOrder] = self.scaler_qualiOrder.transform(train[self.qualiOrder])          
            
            return train
        
        ## Proper Learning & Learning Features
        def fit(self, print_results = True, retourne = False):
            # Entrainement du RF
            self.rf = self.rf.fit(self.train, self.target)
            
            # Résultats
            if print_results:
                if retourne:
                    mat, confusion, prec,recall,giny = self.predict_with_result(None, None, learning_data = True, print_ = True, retourne = retourne)
                    return mat, confusion, prec,recall,giny
                else:
                    self.predict_with_result(None, None, learning_data = True, print_ = True, retourne = False)
        
        
        ## Test & Performance Features
    
        def predict_with_result(self, test, y_true, learning_data = False, print_ = True, retourne = False):
            if learning_data:
                y_true = self.target
            y_pred = self.predict(test, probas = False, learning_data = learning_data)
            confusion = metrics.confusion_matrix(y_true, y_pred)
            prec = metrics.accuracy_score(y_true, y_pred)
            
            
            if len(y_true[y_true==self.positive_class]) ==0:
                recall = float('nan')
                giny = float('nan')
            else:
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
            
        
        def predict(self, test, learning_data = False, probas = False):
            
            # Données de test provenant de l'entrainement ou pas
            if learning_data:
                test = self.train
            else:
                test = self.preprocessing_test(test)
                # Ajout des modalités de variables qualitatives qui ne seraitent pas présents dans l'échantiilon de test: on le met à zéro
                for col in self.cols:
                    if col not in test.columns:
                        test[col] = 0
            
            # Colonnes dans le bon ordre
            test = test[self.cols]
                    
            # Prédiction de classes ou probas
            if probas:
                pred = self.rf.predict_proba(test)
            else:
                 pred = self.rf.predict(test)
    
            return pred
        
        
        def feature_importances_(self, plot = True):
            importances = pd.DataFrame(columns=self.train.columns)
            importances.loc[0] = self.rf.feature_importances_
            importances = importances.mean(axis = 0).sort_values(ascending = False)
            baro.importance = importances
            if plot:
                importances.plot.bar(figsize = (10,7))
            return importances
        
        
        ## Save & Load Models
        def save(self, path):
            # Open the file to save as pkl file
            file = open(path, 'wb')
            pickle.dump(self, file)
            file.close()
        
        def load(self, path):
            return 0
    
    baro_ = barometre_accidents_corporels(dataC, QUANTI_KEEP, QUALI_NOORDER_KEEP, QUALI_ORDERED_KEEP)
    
    baro_.fit(print_results = True)
    
    importance = baro_.feature_importances_(plot = False)
    # Sauvegarde sur disque-dur
    path = 'Barometre_corporel_200_60_60_Alexis.pkl'
    baro_.save(path)