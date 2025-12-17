# Projet ingénierie des données 
Le but du projet est de partir de deux ou 3 datasets public et de constuire un nouveau dataset avec des informations agrées qui apporte un plus value.

## Problèmatique 
## Sujet abordé : Parc éolien 
Analyser la consommation électrique régionale au regard du mix de production électrique local, en particulier l’éolien, afin d’identifier si les régions productrices d’électricité éolienne sont également consommatrices nettes de cette énergie ou si celle-ci est redistribuée vers d’autres régions.

- Où se situent les parcs éoliens ?
- Combien produisent-ils par région ?
- Quelle est la consommation électrique de ces régions ?
- Le volume produit localement est-il supérieur ou inférieur à la consommation régionale ?


## Datasets 
### Consommation annuelle d'electricite et gaz par région
- Source : https://www.data.gouv.fr/datasets/consommation-annuelle-delectricite-et-gaz-par-region/
- description : permet d’analyser l’évolution annuelle de la consommation d’énergie (électricité et gaz) en France, ventilée par région ainsi que par d’autres dimensions comme les secteurs d’activité et les catégories de consommation.
- Années couvertes : 2011 à 2024



### Production electrique annuelle par filiere à la maille iris 
- Source : https://www.data.gouv.fr/datasets/production-electrique-annuelle-par-filiere-a-la-maille-iris/7
- description : fournit des informations sur la production électrique annuelle, ventilée par filière de production et par IRIS (îlots regroupés pour l’information statistique), pour le réseau géré par Enedis.
- Période couverte : de 2011 à 2023.
- Diffusé par : Enedis (gestionnaire de réseau).
#### Contenu 
Ce dataset contient, pour chaque IRIS et chaque année :
- Production électrique annuelle totale (en unités d’énergie, typiquement en kWh ou MWh selon les fichiers). 
- Nombre de sites de production par filière. 
- Filière de production — typiquement des catégories comme :
    - Énergie solaire photovoltaïque
    - Éolien
    - Hydraulique
    - Biomasse
    - Autres filières électriques (selon structuration des données). 
- Domaine de tension et de puissance des sites (classification technique indiquant la taille ou l’ampleur des installations).


### Fichiers de données conso annuelles 
- source : https://www.data.gouv.fr/datasets/consommation-annuelle-delectricite-et-gaz-par-region/
- Description : regroupe des données de consommation annuelle d’énergie en France.
Il s’inscrit dans le jeu de données « Consommation annuelle d’électricité et de gaz » diffusé sur data.gouv.fr et produit à partir des informations collectées par les gestionnaires de réseaux d’énergie.
- Période couverte : 2011 à 2024

#### Contenu 
Pour chaque ligne, on retrouve généralement :
- Année ===> Données annuelles
- Maille géographique
- Code géographique (INSEE ou équivalent)
- Libellé de la zone
- Type d’énergie
- Électricité
- Gaz
- Secteur de consommation
- Résidentiel
- Tertiaire
- Industriel
- Autres secteurs (selon disponibilité)
- Indicateurs de consommation
- Consommation annuelle (en kWh ou MWh)
- Nombre de points de livraison ou de sites (si présent)


## TODO : 
- finir la doc
- ajouter les autres doc de transmission entre région
