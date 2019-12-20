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



if __name__ == "__main__":
    
    df = pd.read_csv("./ground_truth.csv", parse_dates=["date"])
    df["id_user"]=df["id_user"].astype(str)
    df["month"]=df["date"].dt.month
    df["year"]=df["date"].dt.year

    dx = pd.read_csv("./atxf.csv", parse_dates=["date"])
    dx["month"]=dx["date"].dt.month
    dx["year"]=dx["date"].dt.year

    all_anon = generateLists(dx)
    with open('all_clear', 'wb') as fp:
        pickle.dump(all_clear, fp)
