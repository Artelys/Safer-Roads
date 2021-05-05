![BEGOOD](images/BEGOOD.png)
![loiret](images/loiret.png)
# Safer-roads

## Présentation du projet


Le Département du Loiret est un des partenaires du projet européen INTERREG NWE BE-GOOD, qui vise à la réutilisation des informations du secteur public dans le domaine des infrastructures et de l’environnement par les jeunes entreprises.


Le Département du Loiret était porteur du défi « SAFER ROADS » dont Artelys était lauréat.


Le but du projet est d’apporter des éléments de décision afin d’améliorer la sécurité routière grâce à un traitement intelligent des données d’accidents de la route. Pour capitaliser sur une mise en commun de sources de données pertinentes, plusieurs acteurs de la mobilité ont collaboré sur ce projet : services publics du Département (SIG, Observatoire de la Route, gestion des infrastructures), forces de l’ordre (Gendarmerie Nationale), pompiers (SDIS), assureurs et assisteurs (Mondial Assistance, Thelem) et services de navigation grand public (Waze, Coyote).


Artelys a développé un prototype sous la forme d’une plateforme web qui pourra être mise à disposition des pouvoirs publics, des parties prenantes du projet mais aussi du citoyen. Son objectif est de mener une analyse globale sur les spécificités des routes du Loiret, mais aussi envisager des actions préventives.
Ce projet est cofinancé par le projet Interreg North West Europe BE-GOOD.


## Notes de version

### Version 1.0


Première version des plugins kibana **data-management** et **risk-prediction** et du serveur Python flask.

## Installation

### Pré-requis


Installation de docker et docker-compose.

### Compilation des plugins Kibana


Cette étape n'est nécessaire que si vous souhaitez reconstruire les plugins kibana. Sinon, vous pouvez utiliser par défaut ceux précompilés situés dans le répertoire kibana/répertoire

Si vous avez déjà exécuté cette opération auparavant, n'hésitez pas à élaguer votre constructeur via la commande : **docker builder prune**

Dans le répertoire kibana, lancer les commandes suivantes successivement :

```bash
cd kibana
docker build . -f Dockerfile.build -t artelys/saferroads/buildkibana:1.0
docker-compose up
```

Ceci permettra de générer dans le répertoire **kibana/build** deux zip <em>data_management</em> et <em>risk_prediction</em>

#### Exécution ####


Afin d'exécuter l'ensemble des composants, allez dans le répertoire **docker** et exécutez la commande :

```bash
docker-compose up -d
```


Ceci permettra de démarrer elasticsearch, kibana et le serveur python flask.


Tout est configuré pour démarrer correctement avec docker.


Si vous souhaitez ajouter des paramètres à elasticsearch, comme le système d'authentification, vous devez changer la méthode **get_connection** dans flask.

### Tester


Aller sur l'URL <http://localhost:5601> dans un navigateur.


### Gestion des donnnées

## Description du plugin Data management


**data-management** : Ce plugin permet la gestion de données, soit l’ajout et la mise à jour et harmonisation des données de l'ensemble des données de voirie, accidents corporels, matériels, incidents basés sur des données collaboratives, météo…

L'harmonisation des données consiste à harmoniser les données pouvant être hétérogènes pour être techniquement exploitables.

Trois éléments sont nécessaires pour intégrer les données :

- Le chemin du fichier de données

- Le type du document

- Le supplément de nom pour l’index


Les documents doivent avoir un format bien précis selon le type choisi. Le répertoire **data** contient des csv avec un exemple d'une ligne au bon format pour chaque type de données à intégrer.
Ce format doit être respecté pour ne pas obtenir d’erreur.

L'import des données doit se faire dans l'ordre suivant :

1. Données de voirie : soit le fichiers **here.geojson**
2. Données d'occupation de sols : **corine.geojson**, **admin.geojson** et **street_pattern.csv**
3. Données de contexte : soit les données de météo **meteo.csv** et de trafic **trafic.csv**
4. Données d'accidents matériels et corporels : soit les fichiers **sdis.csv**, **mondial.csv** pour les accidents matériels et le fichier **baac.csv** et **corporels.csv** pour les accidents corporels ainsi que les fichiers **waze.csv** et **coyote.csv**


## Description du plugin Risk predicition


**risk-prediction** : Ce plugin contient l'ensemble des scripts permettant de lancer la calibration (entrainement du modèle) et la prévision du risque des accidents.
La prévision du risque est faite pour les accidents matériels et les accidents corporels sur tous les tronçons de routes. Certaines données sont nécessaires afin de prévoir le risque, à savoir :

- La date et le créneau horaire (journée, soir …)
- Les données météo : Température, Pluviométrie, Pression, Direction et vitesse du vent

## Description du flask

Le serveur python contient les fichiers et répertoires suivants:

- **cluster** : Contient le gestionnaire d'administration du site
- **clusters** : Contient l'apparence du site : Images et CSS
- **data** : Contient les données CSV exemple
- **harmonisation** : Contient les scripts d'harmonisation des données
- **send** : Ces scripts permettent d’ajouter les données dans elasticsearch.
- **training** : ce répertoire contient les scripts de calibration des données et le modèle de prévision de risque retenu.


## Contributeurs


Alexis Boutet, Nicolas Megel, Thomas Lefeure, Hafssa Chattou, Alexandre Marie, Paul-Alexandre Bouhana
