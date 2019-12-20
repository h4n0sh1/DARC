import pandas as pd
import numpy as np
import copy as cp
import sys
import pickle
import hashlib as hs
import base64 as b64
import os
import random as rd
import datetime as dt
import threading
import time 
from difflib import SequenceMatcher
from darc_core.metrics import Metrics
from darc_core.preprocessing import round1_preprocessing
from darc_core.utils import check_format_trans_file


def sort_score(hash, score, matching_score):
    new_score = matching_score[:]

    ## score > score1
    if (score > matching_score[0][1]):
        ## remplacement de hash3 et score3 par hash2 et score2
        new_score[2][0] = new_score[1][0]
        new_score[2][1] = new_score[1][1]
        ## remplacement de hash2 et score2 par hash1 et score1
        new_score[1][0] = new_score[0][0]
        new_score[1][1] = new_score[0][1]
        ## remplacement de hash1 et score1 par score et hash
        new_score[0][0] = hash
        new_score[0][1] = score

    ## score > score2
    elif (score > matching_score[1][1]):
        ## remplacement de hash3 et score3 par hash2 et score2
        new_score[2][0] = new_score[1][0]
        new_score[2][1] = new_score[1][1]
        ## remplacement de hash2 et score2 par hash et score
        new_score[1][0] = hash
        new_score[1][1] = score

    ## score > score3
    elif (score > matching_score[2][1]):
        ## remplacement de hash3 et score3 par hash et score
        new_score[2][0] = hash
        new_score[2][1] = score

    return new_score


def calcul_matching_score(clear_signature, anon_signature):
    nb_matching_item = 0
    copy_anon=anon_signature.copy()
    for item in clear_signature:
        i = 0
        while (i < len(copy_anon)):
            if (item == copy_anon[i]):
                nb_matching_item += 1
                copy_anon.pop(i)
                break
            i += 1

    return (2 * nb_matching_item) / (len(clear_signature) + len(anon_signature))


def match_hash_to_user(clear_signature, anon_signature, seuil):
    
    ## initialisation variables
    anon_size = len(anon_signature)
    resultat_matching = []

    ## parcours des differentes signatures dans clear_signature
    for signature in clear_signature:
        matching_score = [["hash1", 0], ["hash2", 0], ["hash3", 0]]
        i = 0
        while ((i < anon_size) and (matching_score[0][1] < seuil)):
            ## calcul du score de matching (seulement si le hash n'a pas deja ete atttribue avec certitude)
            score =  calcul_matching_score(signature[1], anon_signature[i][1])
            matching_score = sort_score(anon_signature[i][0], score, matching_score)
            i += 1
        resultat_matching.append([signature[0], matching_score])

    return resultat_matching



def partition(df,y,m,u):
    return df[(df["id_user"]==u) & (df["month"]==m) & (df["year"]==y)]["id_item"]

def extractListForMonth(df,y,m):
    users = df[(df["month"]==m) & (df["year"]==y)]["id_user"].unique().tolist()
    months =  list(range(1,13))

    key = []
    hashed = []

    for u in users:
        key=[]
        partitioned = partition(df,y,m,u)
        key=partitioned.unique().tolist()
        hashed.append([u,key,len(key)])

    hashed=sorted(hashed, key = lambda x:x[2])
    return hashed

def generateLists(df):
    lists_all=[]
    lists_all.append(extractListForMonth(df,2010,12))
    for m in range(1,13):
        lists_all.append(extractListForMonth(df,2011,m))
    return lists_all


def makeMatches(all_clear,all_anon):

    solution = {}
    solution[0] = match_hash_to_user(all_clear[0], all_anon[0], 0.9)
    print("Iteration 1/13 \n")

    for i in range(1,(len(all_anon))):
        solution[i] = match_hash_to_user(all_clear[i], all_anon[i], 0.9)
        print("Iteration ", (i+1) , "/13 \n")

    return solution


def generateF(df):
    users=df["id_user"].unique().tolist()
    months=list(range(1,14))
    F = pd.DataFrame(index=users,columns=months)
    for col in F.columns:
        F[col].values[:] = "DEL"
    F_dict = F.T.to_dict('list')

    return F,F_dict


def populateF(solution,F_dict):

    for month,match in solution.items():
        for m in match:
            F_dict[str(m[0])][month]=m[1][0][0]


    F = pd.DataFrame.from_dict(F_dict,orient="index")

    return F


if __name__ == "__main__":

    print('*'*100 , "\n", " "*30, "iK4n Reidentify Everything, even your house\n", '*'*100, '\n')
    
    print("Generating the fun tables \n")
    df = pd.read_csv("./ground_truth.csv", parse_dates=["date"])
    df["id_user"]=df["id_user"].astype(str)
    df["month"]=df["date"].dt.month
    df["year"]=df["date"].dt.year

    dx = pd.read_csv("k4nU.csv", parse_dates=["date"])
    dx["month"]=dx["date"].dt.month
    dx["year"]=dx["date"].dt.year

    print("Loading the all_clear Lists \n")
    with open ('all_clear', 'rb') as ac:
        all_clear = pickle.load(ac)

    print( " Generating the all_anon Lists \n" )
    all_anon = generateLists(dx)
    
    print( " Doing the big boy stuff \n")
    solution = makeMatches(all_clear,all_anon)
    
    print( " Generating the matrice F \n")
    F,F_dict = generateF(df)
    
    print("Populating the matrice F \n")
    F = populateF(solution,F_dict)

    print("Exporting the result to csv \n")
    F.to_csv("F_truth1.csv", index=True)
    ex=pd.read_csv("F_truth1.csv", index_col="Unnamed: 0")
    print(ex.head())
    

