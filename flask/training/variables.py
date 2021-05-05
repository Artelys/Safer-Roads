# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 11:45:49 2020

@author: aboutet
"""

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
    
NODES_1 = 'REF_IN_ID'
NODES_2 = 'NREF_IN_ID'
ADJ_SUP = "Vitesse adjacente supérieure"
ADJ_INF = "Vitesse adjacente inférieure"

TRAFIC = 'Trafic redressé'

# Corine Land Cover
CORINE = 'CORINE'

MATERIEL = "Total materiel"

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
