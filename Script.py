# Utilisation du logiciel Neo4J sur le speed-dating dans le cadre d'un projet ingénieur M2
# Date du projet : novembre 2022
# Auteurs : 
# - Marie JOIGNEAU marie.joigneau@agrocampus-ouest.fr
# - Laura PAPER laura.paper@agrocampus-ouest.fr

#==========================================================================
# INTRODUCTION
#==========================================================================

print("----------------------------------------")
print("INTRODUCTION")
print("----------------------------------------")




from py2neo import *
import pandas as pd
from pandas import DataFrame
import os, sys
import math
import matplotlib.pyplot as plt
import numpy as np

# we change the directory:
print("Current working dir : %s" % os.getcwd())
os.chdir("D:/OneDrive/ACO 3eme annee 2022-2023/big data - christine largouët/projet")

# Connexion au graphe
graph = Graph("bolt://localhost:7687", auth=("neo4j", "Marie"))

# petit nettoyage de la base avant de commencer
graph.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")

# ==========================================================================
# PRE TRAITEMENT
# ==========================================================================

print("----------------------------------------")
print("PRE TRAITEMENT")
print("----------------------------------------")

# -------------------- 1) Lecture du dataframe -----------------------
print(" -------------------- 1) Lecture du dataframe ----------------------- ")

# lecture du fichier csv des individus :
nRowsRead = 7410  # On prend les 20 premieres vagues (7411 lignes / 8378)
df = pd.read_csv('Speed Dating Data_original.csv', delimiter=',', nrows=nRowsRead)

# On a 195 variables pour 7411 lignes :
nRow, nCol = df.shape
print(f'Il y a {nRow} individus et {nCol} variables')
print("len df %(len)d " % {"len": len(df)})

# -------------------- 2) Ajout de id aux variables caractérisant l'identification des individus -----------------------
print(
    " -------------------- 2) Ajout de id aux variables caractérisant l'identification des individus ----------------------- ")

# On va rajouter "id" à "iid" et "pid" pour ne pas que le chiffre soit mélangé à d'autres pour les graphes
# Ainsi iid=1 va devenir iid=id1 par exemple
print("df")
pid_nan = []
for i in range(len(df)):

    # a) On remplace les iid = 3 par ex par iid = id3
    df['iid'][i] = "id" + str(df['iid'][i])  # df['iid'][1] = 1

    # b) On fait de même pour les pid = 3 qui deviennent id3
    # Pour la colonne pid (identifiant des partenaires), on a des nan, et le numéro est un float, on procède donc autrement:
    if math.isnan(df['pid'][i]) == False:  # la conversion ne marche pas avec des nan
        df['pid'][i] = "id" + str(int(df['pid'][i]))  # df['pid'][0] = 11.0 => on convertie en int()
    else:  # pid_nan = liste utile pour plus tard
        pid_nan.append(i)

print(df['iid'].head())
print(df['pid'].head())
print(pid_nan)

# -------------------- 3) Création de la variable total_matches pour le nombre total de match par individu -----------------------
print(
    " -------------------- 3) Création de la variable total_matches pour le nombre total de match par individu ----------------------- ")

# On transform tous les NaN de la variable match (= 1 si match, 0 sinon) en 0
df['match'] = df['match'].fillna(0)

total_match = []  # vecteur du total des match par individu
tot = int(df.match[0])
nb = 1
n = len(df)  # 7410 lignes
for i in range(1, n):  # pour chaque ligne
    if df.iid[i] == df.iid[i - 1]:  # si c'est le même individu
        tot = tot + int(df.match[i])  # on additionne les match (0 pour non, 1 pour oui)
        nb = nb + 1
    else:
        for j in range(nb):  # on repète la variable pour qu'il y en ait dans chacune des lignes de l'indiviu
            total_match.append(tot)
        tot = int(df.match[i])
        nb = 1
    if i == (len(df) - 1):
        print("i final %(i)d " % {"i": i})
        for j in range(nb):
            total_match.append(tot)

print(total_match)
len(total_match)  # on a bien 507 individus

# On créé la nouvelle variable grâce au vecteur :
df = df.assign(total_matches=total_match)

# -------------------- 4) Création de la variable pmatches pour le % de match réussi par individu -----------------------
print(
    " -------------------- 4) Création de la variable pmatches pour le % de match réussi par individu ----------------------- ")

# calcul du pourcentage de match :
df = df.astype({'total_matches': int, 'round': int})  # on convertie en integer
df = df.assign(pmatches=1)
df['pmatches'] = (df['total_matches'] / df[
    'round']) * 100  # % match = nombre total de matches réussis / nombre de personnes rencontrés
df = df.astype({'pmatches': int})
df.head()

# -------------------- 5) Préparation des variables pour les hobbies (utile pour un futur graphe) -----------------------
print(" -------------------- 5) Préparation des variables pour les hobbies ----------------------- ")

# On s'occupe des potentiels NaN :
df['sports'] = df['sports'].fillna(0)
df['tvsports'] = df['tvsports'].fillna(0)
df['exercise'] = df['exercise'].fillna(0)
df['dining'] = df['dining'].fillna(0)
df["museums"] = df["museums"].fillna(0)
df["art"] = df["art"].fillna(0)
df["hiking"] = df["hiking"].fillna(0)
df["art"] = df["art"].fillna(0)
df["clubbing"] = df["clubbing"].fillna(0)
df["reading"] = df["reading"].fillna(0)
df["tv"] = df["tv"].fillna(0)
df["theater"] = df["theater"].fillna(0)
df["movies"] = df["movies"].fillna(0)
df["concerts"] = df["concerts"].fillna(0)
df["music"] = df["music"].fillna(0)
df["shopping"] = df["shopping"].fillna(0)
df["yoga"] = df["yoga"].fillna(0)

# On convertie en integer :
df = df.astype(
    {'sports': int, 'tvsports': int, 'exercise': int, "dining": int, "museums": int, "art": int, "hiking": int,
     "clubbing": int, "reading": int, "tv": int, "theater": int, "movies": int, "concerts": int, "music": int,
     "shopping": int, "yoga": int})
df.tvsports.head()

# -------------------- 6) On s'occupe des doublons dans le field -----------------------
print(" -------------------- 6) On s'occupe des doublons dans le field ----------------------- ")

df_field_unique = df.drop_duplicates(subset=["field"])
print(df_field_unique['field'].head())
df.field[df.field == "law"] = "Law"

# -------------------- 7) df_unique : 1 ligne par individu -----------------------
print(" -------------------- 7) df_unique : 1 ligne par individu ----------------------- ")

# On prend 1 ligne par individu :
# On a donc pas pris les informations comme comment il a noté le partenaire car ça fait plusieurs lignes (pour chaque partenaire)
print("df_unique")
df_unique = df.drop_duplicates(subset=["iid"])
print(df_unique.head())
# On a 518 individus
len(df_unique)
df_unique['iid'].head()

# -------------------- 8) df_ville_unique : 1 ligne par ville -----------------------
print("-------------------- 8) df_ville_unique : 1 ligne par ville -----------------------")

# On prend 1 ligne par ville :
print("df_ville")
df_ville_unique = df_unique.drop_duplicates(subset=["from"])
print(df_ville_unique['from'].head())
# On a 255 villes
len(df_ville_unique)
df_ville_unique['iid'].head()

# -------------------- 9) df_wave_unique : 1 ligne par vague -----------------------
print("-------------------- 9) df_wave_unique : 1 ligne par vague -----------------------")
# On prend 1 ligne par vague :
print("df_ville")
df_wave_unique = df_unique.drop_duplicates(subset=["wave"])
print(df_wave_unique['wave'].head())
# On a 20 vagues
len(df_wave_unique)

# -------------------- 10) df_meet : 1 ligne 1 rencontre -----------------------
print("-------------------- 10) df_meet : 1 ligne 1 rencontre -----------------------")

# On garde que les rencontres sans nan pour l'id du partenaire (pareil pour les matches)
df_meet = df
df_meet = df_meet.drop(pid_nan)  # vecteur réalisé plus haut
type(pid_nan)
len(df)  # 7410
len(df_meet)  # 7400

# -------------------- 11) df_interet : 1 ligne 1 interet positif -----------------------
print("-------------------- 11) df_interet : 1 ligne 1 interet positif -----------------------")

# On garde que les lignes avec un match :
len(df)
df_interet = df.drop(pid_nan)
df_interet = df_interet[df.dec != 0]  # colonne dec => si 0, le sujet n'a pas décidé de revoir le partenaire
# On a 3160 marques d'intérêt :
len(df_interet)  # 3157

# -------------------- 12) df_match : 1 ligne 1 match réussi -----------------------
print("-------------------- 12) df_match : 1 ligne 1 match réussi -----------------------")

# On garde que les lignes avec un match :
df_match = df[df.match != 0]  # colonne match => si 0 ce n'est pas un match

# -------------------- 13) df_field_unique : 1 ligne 1 domaine de travail -----------------------
print("-------------------- 13) df_field_unique : 1 ligne 1 domaine de travail -----------------------")

# On prend 1 ligne par domaine de travail :
df_field_unique = df.drop_duplicates(subset=["field"])
print(df_field_unique['field'].head())
# On a 243 domaines
len(df_field_unique)

# ==========================================================================
# CREATION DES NOEUDS
# ==========================================================================

print("----------------------------------------")
print("CREATION DES NOEUDS")
print("----------------------------------------")

# -------------------- 1) Noeuds Individu -----------------------
print(" -------------------- 1) Noeuds Individu ----------------------- ")

# On garde que les informations qui caractérisent l'individu :
users_unique = {}
for index, row in df_unique.iterrows():
    users_unique[row['iid']] = Node("Individu",
                                    iid=str(row['iid']),  # Identifiant unique du sujet
                                    id_vague=str(row['id']),  # Identifiant du sujet au sein d'une vague
                                    genre=str(row["gender"]),  # Genre (0 = femme / 1 = homme)
                                    id_genre=str(row["idg"]),  # Identifiant du sujet au sein du genre

                                    condtn=str(row["condtn"]),  # 1 = choix limité / 2 = étendu
                                    vague=str(row["wave"]),  # Vague
                                    nb_people_wave=str(row["round"]),
                                    # Nombre de personnes qui se sont rencontré dans la vague

                                    # Caractéristiques du sujet :
                                    age=str(row["age"]),  # Age du sujet
                                    domaine=str(row["field"]),  # Domaine du travail
                                    domaine_code=str(row["field_cd"]),  # Domaine du travail (codé)
                                    race=str(row["race"]),  # Race du sujet
                                    importance_race=str(row["imprace"]),
                                    # Importance de la race pour le sujet (de 1 à 10)
                                    importance_religion=str(row["imprelig"]),
                                    # Importance de la religion pour le sujet (de 1 à 10)
                                    origine=str(row["from"]),  # Ville d'origine du sujet
                                    origine_code=str(row["zipcode"]),  # Ville d'origine du sujet (codé)
                                    revenu=str(row["income"]),  # Revenu du sujet
                                    but=str(row["goal"]),  # But recherché dans le speed-dating

                                    freq_date=str(row["date"]),
                                    # A quelle fréquence sort le sujet va en date? (de 1 à 7)
                                    freq_dehors=str(row["go_out"]),  # A quelle fréquence le sujet sort? (de 1 à 7)

                                    # Carrière visées :
                                    but_carriere=str(row["career"]),  # Carrière visée par le sujet?
                                    but_carriere_code=str(row["career_c"]),  # Carrière visée par le sujet (code) ?

                                    # Intérêt du sujet sur des activités (de 1 à 10) :
                                    int_sport=str(row["sports"]),  # Intérêt du sujet pour faire du sport
                                    int_tvsports=str(row["tvsports"]),
                                    # Intérêt du sujet pour regarder les sports à la télévision
                                    int_exercice=str(row["exercise"]),
                                    # Intérêt du sujet pour exercer du sport / musculation
                                    int_diner=str(row["dining"]),  # Intérêt du sujet pour les diners dehors
                                    int_musee=str(row["museums"]),  # Intérêt du sujet pour les musées / galleries
                                    int_art=str(row["art"]),  # Intérêt du sujet pour l'art
                                    int_hiking=str(row["hiking"]),  # Intérêt du sujet pour la randonnée / le camping
                                    int_clubbing=str(row["clubbing"]),  # Intérêt du sujet pour sortir en boite / dancer
                                    int_reading=str(row["reading"]),  # Intérêt du sujet pour la lecture
                                    int_tv=str(row["tv"]),  # Intérêt du sujet pour la télévision
                                    int_theatre=str(row["theater"]),  # Intérêt du sujet pour le théâtre
                                    int_film=str(row["movies"]),
                                    int_concert=str(row["concerts"]),
                                    int_music=str(row["music"]),
                                    int_shopping=str(row["shopping"]),
                                    int_yoga=str(row["yoga"]),  # Intérêt du sujet pour le yoga / la méditation

                                    exp_int=str(row["expnum"]),
                                    # Sur les 20 personnes, combien le sujet pense qu'ils vont être intéressé pour sortir avec lui

                                    # Ce que le sujet recherche dans le sexe opposé :
                                    recherche_att=str(row["attr1_1"]),
                                    recherche_sinc=str(row["sinc1_1"]),
                                    recherche_int=str(row["intel1_1"]),
                                    recherche_fun=str(row["fun1_1"]),
                                    recherche_amb=str(row["amb1_1"]),
                                    recherche_shar=str(row["shar1_1"]),

                                    # Ce que le sujet pense que la plupart des gens recherchent dans un date :
                                    all_recherche_att=str(row["attr4_1"]),
                                    all_recherche_sinc=str(row["sinc4_1"]),
                                    all_recherche_int=str(row["intel4_1"]),
                                    all_recherche_fun=str(row["fun4_1"]),
                                    all_recherche_amb=str(row["amb4_1"]),
                                    all_recherche_shar=str(row["shar4_1"]),

                                    # Ce que le sujet pense que le sexe opposé recherche dans un date :
                                    opp_recherche_att=str(row["attr2_1"]),
                                    opp_recherche_sinc=str(row["sinc2_1"]),
                                    opp_recherche_int=str(row["intel2_1"]),
                                    opp_recherche_fun=str(row["fun2_1"]),
                                    opp_recherche_amb=str(row["amb2_1"]),
                                    opp_recherche_shar=str(row["shar2_1"]),

                                    # Comment le sujet mesure :
                                    mesure_att=str(row["attr3_1"]),
                                    mesure_sinc=str(row["sinc3_1"]),
                                    mesure_int=str(row["intel3_1"]),
                                    mesure_fun=str(row["fun3_1"]),
                                    mesure_amb=str(row["amb3_1"]),

                                    # Comment le sujet pense que les autres le perçoivent
                                    autre_att=str(row["attr5_1"]),
                                    autre_sinc=str(row["sinc5_1"]),
                                    autre_int=str(row["intel5_1"]),
                                    autre_fun=str(row["fun5_1"]),
                                    autre_amb=str(row["amb5_1"]),

                                    tot_think_match=str(row["match_es"]),  # Total des matches que le sujet pense avoir
                                    tot_match=str(row["total_matches"]),  # Total des matches du sujet
                                    pourc_match=int(row[
                                                        "pmatches"]))  # Pourcentage de match du sujet sur le nombre total de personnes rencontrées

# -------------------- 2) Noeuds Ville -----------------------
print(" -------------------- 2) Noeuds Ville ----------------------- ")

# On garde que les informations qui caractérisent la ville :
ville_unique = {}
for index, row in df_ville_unique.iterrows():
    ville_unique[row['from']] = Node("Ville",
                                     origine=str(row["from"]),
                                     origine_code=str(row["zipcode"]))  # Ville d'origine du sujet (codé) : NA => 0

# -------------------- 3) Noeuds Vague -----------------------
print("-------------------- 3) Noeuds Vague -----------------------")

# On garde que les informations qui caractérisent la vague :
wave_unique = {}
for index, row in df_wave_unique.iterrows():
    wave_unique[row['wave']] = Node("Vague",
                                    wave=str(row["wave"]))

# -------------------- 4) Noeud Domaine -----------------------
print("-------------------- 4) Noeud Domaine -----------------------")

# On garde que les informations qui caractérisent le domaine d'activité :
field_unique = {}
for index, row in df_field_unique.iterrows():
    field_unique[row['field']] = Node("Domaine",
                                      domaine=str(row["field"]),  # Domaine du travail
                                      domaine_code=str(row["field_cd"]))  # Domaine du travail (codé)

# ==========================================================================
# CREATION DES RELATIONS
# ==========================================================================

print("----------------------------------------")
print("CREATION DES RELATIONS")
print("----------------------------------------")

rel = []

print("relation VIT")
# Relation VIT entre sujet et ville: permet de voir les sujets d'une même ville
for index, row in df_unique.iterrows():
    fr = users_unique[row['iid']]
    to = ville_unique[row['from']]
    rel.append(Relationship(fr, "VIT", to))

print("relation VAGUE")
# Relation VAGUE : permet de voir les sujets d'une même vague
for index, row in df_unique.iterrows():
    fr = users_unique[row['iid']]
    to = wave_unique[row['wave']]
    rel.append(Relationship(fr, "VAGUE", to))

print("relation MEET")
# Relation MEET : permet de relier 2 personnes qui ont eu un date
for index, row in df_meet.iterrows():
    fr = users_unique[row['iid']]
    to = users_unique[row['pid']]
    rel.append(Relationship(fr, "RENCONTRE", to))

print("relation INTERESSE")
# Relation MATCH : permet de mettre la flèche entre une personne intéressée et la personne qui l'intéresse
for index, row in df_interet.iterrows():
    fr = users_unique[row['iid']]
    to = users_unique[row['pid']]
    rel.append(Relationship(fr, "INTERESSE", to))

print("relation MATCH")
# Relation MATCH : permet de relier 2 personnes entre elles quand il y a un match
for index, row in df_match.iterrows():
    fr = users_unique[row['iid']]
    to = users_unique[row['pid']]
    rel.append(Relationship(fr, "MATCH", to))

print("relation TRAVAILLE")
# Relation TRAVAILLE : permet de voir les sujets qui ont le même travail
for index, row in df_unique.iterrows():
    fr = users_unique[row['iid']]
    to = field_unique[row['field']]
    rel.append(Relationship(fr, "TRAVAILLE", to))

# ==========================================================================
# CREATION DU GRAPHE DANS LA BASE
# ==========================================================================

print("----------------------------------------")
print("CREATION DU GRAPHE")
print("----------------------------------------")

for r in rel:
    graph.create(r)

# ==========================================================================
# REQUÊTES CYPHER
# ==========================================================================

print("----------------------------------------")
print("REQUETES CYPHER")
print("----------------------------------------")

# 3 grandes parties de requête :
# 1) Qui a eu le plus de match? Tout sexe confondu, puis parmi les hommes puis les femmes
# 2) L'utilisateur rentre des caractéristiques et on lui donne une liste d'individu qui pourrait matcher selon ces caractéristiques
# 3) L'utilisateur choisit un individu (celui qui a le plus de match ou un choisi) et il peut accéder à des informations dessus



# ---------------1) Partie 1 : les plus gros succès de match -----------------------
# -------------------- 1.1) Requête 1_1 : plus gros succès en général -----------------------
print("-------------------- 1.1) Requête 1_1 : plus gros succès en général-----------------------")

rq1_1 = "MATCH (i:Individu) RETURN i.pourc_match AS pourc_match, i.int_sport AS int_sport, i.int_tvsports as int_tvsports, i.int_exercice AS int_exercice, i.int_diner AS int_diner, i.int_musee AS int_musee, i.int_art AS int_art, i.int_hiking AS int_hiking, i.int_clubbing AS int_clubbing, i.int_reading AS int_reading, i.int_tv AS int_tv, i.int_theatre AS int_theatre, i.int_film AS int_film, i.int_concert AS int_concert, i.int_music AS int_music, i.int_shopping AS int_shopping, i.int_yoga AS int_yoga, i.iid AS iid ORDER BY pourc_match DESC LIMIT 5"
df = graph.run(rq1_1).to_data_frame()
print(df.head())

#    pourc_match int_sport int_tvsports  ... int_shopping int_yoga    iid
# 0           90        10           10  ...            7       10   id19
# 1           80        10            6  ...            6        1   id14
# 2           80         2            2  ...            8        6    id8
# 3           75         4            1  ...            4        3  id416
# 4           70         4            3  ...            8        3    id9

print("Graphe 1_1 requête 1")

df_origin = df
df = df.drop(columns=['int_sport', 'int_tvsports', 'int_exercice', "int_diner", "int_musee", "int_art", "int_hiking",
                      "int_clubbing", "int_reading", "int_tv", "int_theatre", "int_film", "int_concert", "int_music",
                      "int_shopping", "int_yoga"])
df = df.set_index('iid')
print(df.head())
ax = df.plot(kind="bar")
# ax = df.plot.barh(y="pourc_match",color=[np.where(df["genre"]=1, 'g', 'r')])
ax.set_title("Pourcentage de matches réussi pour les 5 individus avec le plus de matchs réussis (en %)")
ax.set_xlabel("Identifiant unique de l'individu")
ax.set_ylabel("Pourcentage de réussite")
# ax = df.plot.barh(y="pourc_match",color=[np.where(df["genre"]=1, 'g', 'r')])
plt.show()

print("Graphe 2 requête 1")

df = df_origin
df = df.set_index('iid')
df = df.drop(columns=['pourc_match'])
df = df.astype({'int_sport': int, 'int_tvsports': int, 'int_exercice': int, "int_diner": int, "int_musee": int,
                "int_art": int, "int_hiking": int, "int_clubbing": int, "int_reading": int, "int_tv": int,
                "int_theatre": int, "int_film": int, "int_concert": int, "int_music": int, "int_shopping": int,
                "int_yoga": int})
print(df.head())
ax = df.plot.barh()
ax.set_title("Intérêts sur diverses activités pour les 5 individus avec le pourcentage de match le plus élevé")
ax.set_xlabel("Identifiant unique de l'individu")
ax.set_ylabel("Intérêt (de 0 à 10)")
plt.show()

# -------------------- 1.2) Requête 1_2 : plus gros succès chez les hommes -----------------------
print("-------------------- 1.2) Requête 1_2 : plus gros succès chez les hommes -----------------------")

## RQ pour les hommes
print("Les 5 hommes qui ont eu le plus de matchs")

rqh = "MATCH (i:Individu {genre : '1'}) RETURN i.pourc_match AS pourc_match, i.int_sport AS int_sport, i.int_tvsports as int_tvsports, i.int_exercice AS int_exercice, i.int_diner AS int_diner, i.int_musee AS int_musee, i.int_art AS int_art, i.int_hiking AS int_hiking, i.int_clubbing AS int_clubbing, i.int_reading AS int_reading, i.int_tv AS int_tv, i.int_theatre AS int_theatre, i.int_film AS int_film, i.int_concert AS int_concert, i.int_music AS int_music, i.int_shopping AS int_shopping, i.int_yoga AS int_yoga, i.iid AS iid ORDER BY pourc_match DESC LIMIT 5"
df = graph.run(rqh).to_data_frame()
print(df.head())

print("Graphe 1 requête 1_2")

df_homme = df
df = df.drop(columns=['int_sport', 'int_tvsports', 'int_exercice', "int_diner", "int_musee", "int_art", "int_hiking",
                      "int_clubbing", "int_reading", "int_tv", "int_theatre", "int_film", "int_concert", "int_music",
                      "int_shopping", "int_yoga"])
df = df.set_index('iid')
print(df.head())
ax = df.plot(kind="bar")
# ax = df.plot.barh(y="pourc_match",color=[np.where(df["genre"]=1, 'g', 'r')])
ax.set_title("Pourcentage de matches réussi pour les 5 hommes avec le plus de matchs réussis (en %)")
ax.set_xlabel("Identifiant unique de l'individu")
ax.set_ylabel("Pourcentage de réussite")
# ax = df.plot.barh(y="pourc_match",color=[np.where(df["genre"]=1, 'g', 'r')])
plt.show()

print("Graphe 2 requête 1_2")

df = df_homme
df = df.set_index('iid')
df = df.drop(columns=['pourc_match'])
df = df.astype({'int_sport': int, 'int_tvsports': int, 'int_exercice': int, "int_diner": int, "int_musee": int,
                "int_art": int, "int_hiking": int, "int_clubbing": int, "int_reading": int, "int_tv": int,
                "int_theatre": int, "int_film": int, "int_concert": int, "int_music": int, "int_shopping": int,
                "int_yoga": int})
print(df.head())
ax = df.plot.barh()
ax.set_title("Intérêts sur diverses activités pour les 5 hommes avec le meilleur pourcentage de match")
ax.set_xlabel("Identifiant unique de l'individu")
ax.set_ylabel("Intérêt (de 0 à 10)")
plt.show()

# -------------------- 1.3) Requête 1_3 : plus gros succès chez les femmes -----------------------
print("-------------------- 1.3) Requête 1_3 : plus gros succès chez les femmes -----------------------")

## RQ pour les hommes
print("Les 5 femmes qui ont eu le plus de matchs")

rqf = "MATCH (i:Individu {genre : '0'}) RETURN i.pourc_match AS pourc_match, i.int_sport AS int_sport, i.int_tvsports as int_tvsports, i.int_exercice AS int_exercice, i.int_diner AS int_diner, i.int_musee AS int_musee, i.int_art AS int_art, i.int_hiking AS int_hiking, i.int_clubbing AS int_clubbing, i.int_reading AS int_reading, i.int_tv AS int_tv, i.int_theatre AS int_theatre, i.int_film AS int_film, i.int_concert AS int_concert, i.int_music AS int_music, i.int_shopping AS int_shopping, i.int_yoga AS int_yoga, i.iid AS iid ORDER BY pourc_match DESC LIMIT 5"
df = graph.run(rqf).to_data_frame()
print(df.head())

print("Graphe 1 requête 1_3")

df_femme = df
df = df.drop(columns=['int_sport', 'int_tvsports', 'int_exercice', "int_diner", "int_musee", "int_art", "int_hiking",
                      "int_clubbing", "int_reading", "int_tv", "int_theatre", "int_film", "int_concert", "int_music",
                      "int_shopping", "int_yoga"])
df = df.set_index('iid')
print(df.head())
ax = df.plot(kind="bar")
# ax = df.plot.barh(y="pourc_match",color=[np.where(df["genre"]=1, 'g', 'r')])
ax.set_title("Pourcentage de matches réussi pour les femmes ayant le plus de match (en %)")
ax.set_xlabel("Identifiant unique de l'individu")
ax.set_ylabel("Pourcentage de réussite")
# ax = df.plot.barh(y="pourc_match",color=[np.where(df["genre"]=1, 'g', 'r')])
plt.show()

print("Graphe 2 requête 1_3")

df = df_femme
df = df.set_index('iid')
df = df.drop(columns=['pourc_match'])
df = df.astype({'int_sport': int, 'int_tvsports': int, 'int_exercice': int, "int_diner": int, "int_musee": int,
                "int_art": int, "int_hiking": int, "int_clubbing": int, "int_reading": int, "int_tv": int,
                "int_theatre": int, "int_film": int, "int_concert": int, "int_music": int, "int_shopping": int,
                "int_yoga": int})
print(df.head())
ax = df.plot.barh()
ax.set_title("Intérêts sur diverses activités pour les 5 femmes avec le pourcentage de match le plus élevé")
ax.set_xlabel("Identifiant unique de l'individu")
ax.set_ylabel("Intérêt (de 0 à 10)")
plt.show()

# --------------- 2) Partie 2 : Individus en fonction de caractéristiques -----------------------
print("-------------------- 2) Partie 2 : Individus en fonction de caractéristiques -----------------------")

print(" Exemple d'individu qui existe => race : 2, goal : 1, domaine : Business, ville : New York, sexe : 1 (iid=18)")

domaine = str(input("Dans quel domaine voulez vous que votre partenaire travaille : "))
ville = str(input("Dans quelle ville voulez vous trouvez votre partenaire : "))
sexe = str(input("Quel est le sexe du partenaire que vous recherchez (1 = homme, 0 = femme) : "))

rq3 = "MATCH (i:Individu {domaine:'" + domaine + "', origine:'" + ville + "', genre:'" + sexe + "'}) RETURN i.iid"

df = graph.run(rq3).to_data_frame()
print(df.head())

# ---------------3) Partie 3 : Choisir un individu et voir des informations -----------------------
# -------------------- 3.1) Requête 3_1 : Ville dans laquelle l'individu vit -----------------------
print("-------------------- 3.1) Requête 3_1 : Ville dans laquelle l'individu vit -----------------------")

print(" l'individu qui a le plus de % de match réussi est " + str(df_origin.iid[0]))
print(" l'homme qui a le plus de % de match réussi est " + str(df_homme.iid[0]))  # A CHANGER
print(" la femme qui a le plus de % de match réussi est " + str(df_femme.iid[0]))  # A CHANGER
votre_individu = str(
    input("Ecrire id + numéro de l'individu pour un individu dont vous voulez connaître son lieu de résidence (ex = id19) : "))

rq2_1 = "MATCH (i:Individu {iid: '" + votre_individu + "'})-[:VIT]->(v:Ville) RETURN v.origine"
df = graph.run(rq2_1).to_data_frame()
print(df.head())

# -------------------- 3.2) Requête 3_2 : Personnes avec qui l'individu a matché -----------------------
print("-------------------- 3.2) Requête 3_2 : Personnes avec qui l'individu a matché -----------------------")

print(" l'individu qui a le plus de % de match réussi est " + str(df_origin.iid[0]))
print(" l'homme qui a le plus de % de match réussi est " + str(df_homme.iid[0]))  # A CHANGER
print(" la femme qui a le plus de % de match réussi est " + str(df_femme.iid[0]))  # A CHANGER
votre_individu = str(
    input("Ecrire id + numéro de l'individu pour lequel vous voulez voir avec qui il a matché (ex = id19) : "))

rq2_2 = "MATCH (i:Individu {iid: '" + votre_individu + "'})-[:MATCH]->(p:Individu) RETURN p.iid"
df = graph.run(rq2_2).to_data_frame()
print(df)

# -------------------- 3.3) Requête 3_3 : Personnes que l'individu a rencontré -----------------------
print("-------------------- 3.3) Requête 3_3 : Personnes que l'individu a rencontré -----------------------")

print(" l'individu qui a le plus de % de match réussi est " + str(df_origin.iid[0]))
print(" l'homme qui a le plus de % de match réussi est " + str(df_homme.iid[0]))  # A CHANGER
print(" la femme qui a le plus de % de match réussi est " + str(df_femme.iid[0]))  # A CHANGER
votre_individu = str(
    input("Ecrire id + numéro de l'individu pour lequel vous voulez voir qui il a rencontré (ex = id19) : "))

rq2_3 = "MATCH (i:Individu {iid: '" + votre_individu + "'})-[:RENCONTRE]->(p:Individu) RETURN p.iid"
df = graph.run(rq2_3).to_data_frame()
print(df)

# -------------------- 3.4) Requête 3_4 : Personnes par qui l'individu est intéressé -----------------------
print("-------------------- 2.4) Requête 2_4 : Personnes par qui l'individu est intéressé -----------------------")
print("Personnes par qui l'individu est intéressé")

print(" l'individu qui a le plus de % de match réussi est " + str(df_origin.iid[0]))
print(" l'homme qui a le plus de % de match réussi est " + str(df_homme.iid[0]))  # A CHANGER
print(" la femme qui a le plus de % de match réussi est " + str(df_femme.iid[0]))  # A CHANGER
votre_individu = str(
    input("Ecrire id + numéro de l'individu pour lequel voulez voir par qui il est interessé (ex = id19) : "))

rq2_4 = "MATCH (i:Individu {iid: '" + votre_individu + "'})-[:INTERESSE]->(p:Individu) RETURN p.iid"
df = graph.run(rq2_4).to_data_frame()
print(df)



# -------------------- 3.5) Requête 3_5 : Dans quel domaine travaillent les personnes qui ont matché avec des personnes travaillant dans un domaine au choix -----------------------
print(
    "-------------------- 3.5) Requête 3_5 : Dans quel domaine travaillent les personnes qui ont matché avec des personnes travaillant dans un domaine au choix -----------------------")


domaine = str(input(
    "Ecrire le domaine de travail pour lequel voulez voir avec quel type de travail ces personnes ont matché (ex = Business) : "))

rq2_6 = "MATCH (i:Individu {domaine: '" + domaine + "'})-[:MATCH]->(p:Individu) RETURN p.domaine "
df = graph.run(rq2_6).to_data_frame()
print(df)






print("----------------")
print("fin du programme")
print("----------------")



