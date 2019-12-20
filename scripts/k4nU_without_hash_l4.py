import pandas as pd
import numpy as np
import copy as cp
import darc_core
import pickle
import sys
import hashlib as hs
import base64 as b64
import os
import random as rd
import datetime as dt
from darc_core.metrics import Metrics
from darc_core.utils import M_COL, T_COL, T_COL_IT, NB_GUESS, SIZE_POOL
from darc_core.preprocessing import round1_preprocessing
from darc_core.utils import check_format_trans_file

def hour_to_int(hours):
    if(isinstance(hours,float)):
        return int(hours)

def parse_dates(date):
    if(isinstance(date,dt.date) and not pd.isnull(date)):
        return date.strftime('%Y/%m/%d')

def retransformDel(dfn):
    dfn_dict = dfn.T.to_dict('list')
    for i in range(0,len(dfn_dict)):
        if(dfn_dict[i][0]=="DEL"):
            dfn_dict[i][1]=""
            dfn_dict[i][2]=""
            dfn_dict[i][3]=""
            dfn_dict[i][4]=""
            dfn_dict[i][5]=""
    dfn = pd.DataFrame.from_dict(dfn_dict,orient="index")
    dfn = dfn.rename(columns={0:"id_user",1:"date",2:"hours",3:"id_item",4:"price",5:"qty",6:"month",7:"year"}) 
    return dfn

def populateF():
    """
        Apply hashme for all rows
    """
    dfn_copy.sort_values(["id_user","month"])
    #salt_table.sort_index(inplace=True)
    dfn["id_user"]=dfn_copy.apply(lambda x: hashme(x["id_user"],x["month"],x["year"]), axis=1)

def generateF(df):
    users=df["id_user"].unique().tolist()
    months=list(range(1,14))
    F = pd.DataFrame(index=users,columns=months)
    for col in F.columns:
        F[col].values[:] = "DEL"
    F_dict = F.T.to_dict('list')
    
    return F,F_dict

def hashme(id_user, month, year): 
    """
        :return: Hash[user,month] = SHA256(Salt[user,month] + id_user)
    """
    if(id_user=="DEL"):
        return id_user
    else:
        hashme.counter+=1
        percent=(hashme.counter/307054)*100 
        sys.stdout.write("\rProgress %i -- Count : %i / 307054" % (percent,hashme.counter))
        sys.stdout.flush()
        if(year==2010 and month==12 ):
            if(F_dict[id_user][0] != "DEL"):
                return F_dict[id_user][0]
            r = hs.sha256(salt_dict[id_user][0] + id_user.encode()).hexdigest()
            F_dict[id_user][0]=r
        else:
            if(F_dict[id_user][month] != "DEL"):
                return F_dict[id_user][month]
            r = hs.sha256(salt_dict[id_user][month] + id_user.encode()).hexdigest()
            F_dict[id_user][month]=r
        return r


def generateSalts():
    """ 
        Generate Salt{256b} Table : Mij = Salt for user i in month j 
    """
    ordered_months = list(range(1,14))
    ids = dfn_copy["id_user"].unique()
    ids = ids[ids != "DEL"]
    salt_table=pd.DataFrame(columns=ordered_months,index=ids)
    salt_table.set_index(ids,inplace=True)
    for i in ids:
        for j in ordered_months:
            # Generates unique b64 values
            salt_table[j][i]=os.urandom(256)
    return salt_table


def build_anonymized_dataset(df,partitions): 
    for p in partitions:
        mean_hour=0
        mean_qty=0
        for i in p:
            mean_hour+=df_dict[i][2]
            mean_qty+=df_dict[i][5]
            shuffleDates(df,p)
        for i in p:
            df_dict[i][2]=mean_hour/len(p)
            df_dict[i][5]=mean_qty/len(p)
    dfn = pd.DataFrame.from_dict(df_dict,orient="index")
    dfn = dfn.rename(columns={0:"id_user",1:"date",2:"hours",3:"id_item",4:"price",5:"qty",6:"month",7:"year"})  
    
    return dfn

def split(df, partition, column): 
    """
        Split df[partition] over column on median 
        :return: List[[IndexRange]]*2 {left ,right}
    """
    dfp = df["id_user"][partition]
    sanitizedPartition = dfp.index[dfp != "DEL"]
    dfp = df[column][sanitizedPartition]
    if (column in categorical):
        values = dfp.unique()
        dfp.sort_values(inplace=True)
        lv=set(values[:len(values)//2])
        rv=set(values[len(values)//2:])
        return dfp.index[dfp.isin(lv)],dfp.index[dfp.isin(rv)]
    else:
        median=dfp.median()
        dfp.sort_values(inplace=True)
        dfl=dfp.index[dfp<median]
        dfr=dfp.index[dfp>=median]
        return(dfl,dfr)

def is_k_anonymous (df, partition, k=1):
    if ((len(partition)<k)):
        return False
    return True

def partition_dataset(df, is_valid):
    """
        Greedy search
        Split dataframe over {year,month,id_item}
        :return: List[[IndexRange]]
    """
    finished_partitions=[]
    partitions=[df.index]
    while partitions:
        partition = partitions.pop(0)
        columns = ["year","month", "id_item"]
        for column in columns:
            lp, rp = split(df,partition,column)
            if not is_valid(df,lp) or not is_valid(df,rp):
                continue
            partitions.extend((lp,rp))
            break
        else:
            finished_partitions.append(partition)
    return finished_partitions

def make_l_diverse(df_dict,partitions,l=4):
    filtered=[]
    for p in finished_partitions:
        array = []
        for i in p:
            if(df_dict[i][0] != "DEL" and df_dict[i][0] not in array):
                array.append(df_dict[i][0])
        if len(array)<l:
            for i in p:
                
                if(df_dict[i][6]==12 and df_dict[i][7]==2010):
                    free_space[0].append(i)    
                else:
                    free_space[df_dict[i][6]].append(i)
                    
                df_dict[i][0]="DEL"
                df_dict[i][1]=df_dict[i][1].strftime("%Y-%m-01")
                df_dict[i][2]=""
                df_dict[i][3]=""
                df_dict[i][4]=""
                df_dict[i][5]=""
        else:
            filtered.append(p)
            
    dfn = pd.DataFrame.from_dict(df_dict,orient="index")
    dfn = dfn.rename(columns={0:"id_user",1:"date",2:"hours",3:"id_item",4:"price",5:"qty",6:"month",7:"year"})  
    
    return dfn,filtered


def shuffleDates(df,partition):
    date_array = []
    date_shuffled = []
    for i in partition:
        date_array.append(df_dict[i][1])
    while(len(date_array)):
        i=rd.randrange(len(date_array))
        date_shuffled.append(date_array[i])
        date_array.pop(i)
    k=0
    for i in partition:
        df_dict[i][1]=date_shuffled[k]
        k+=1

    return 1

def mergeIdentical(df,df_dict):
    """ Merge "same" transaction into one line. Assign "DEL" to duplicates
        Transactions are "identical" if their keys are equal

        :map: key:[id_user,date,id_item] value:[qty_cumulative,count]
    """
    map={}
    for d in range(0,len(df)):
        if(map.get((df["id_user"][d],df["date"][d],df["id_item"][d]))):
            map[(df["id_user"][d],df["date"][d],df["id_item"][d])][0] += df_dict[d][5]
            map[(df["id_user"][d],df["date"][d],df["id_item"][d])][1] += 1
            if(df_dict[d][6]==12 and df_dict[d][7]==2010):
                free_space[0].append(d)
            else:
                free_space[df_dict[d][6]].append(d)
            df_dict[d][0]="DEL"
            df_dict[d][1]=df_dict[d][1].strftime("%Y-%m-01")
            df_dict[d][2]=""
            df_dict[d][3]=""
            df_dict[d][4]=""
            df_dict[d][5]=""
        else:
            map[(df["id_user"][d],df["date"][d],df["id_item"][d])] = [df_dict[d][5],1]
    """
    for d in range(0,len(df)):
        df_dict[d][5]=map[(df["id_user"][d],df["date"][d],df["id_item"][d])][0]
    """
    dfn = pd.DataFrame.from_dict(df_dict,orient="index")
    dfn = dfn.rename(columns={0:"id_user",1:"date",2:"hours",3:"id_item",4:"price",5:"qty",6:"month",7:"year"})

    return dfn ,map

if __name__=="__main__": 

    
    print("Importing DataSet \n")
    df = pd.read_csv("./ground_truth.csv", parse_dates=["date"])
    df["hours"]= pd.to_datetime(df["hours"], format = '%H:%M%S').dt.hour
    df["month"]=df["date"].dt.month
    df["year"]=df["date"].dt.year
    df["id_user"]=df["id_user"].astype(str)
    df["price"]=df["price"].astype("float")
    df["qty"]=df["qty"].astype("float")
    df_dict = df.T.to_dict('list')


    print("Merging Identical lines \n")
    free_space={0:[],1:[],2:[],3:[],4:[],5:[],6:[],7:[],8:[],9:[],10:[],11:[],12:[]}
    dfn, map= mergeIdentical(df,df_dict)
    save_dfn_merge=dfn.copy()
    
    print("Importing Finished Partitions \n") 
    dfn["id_item"]=dfn["id_item"].astype("category")
    categorical = set ({'id_item'})
    with open ('finished_partitions', 'rb') as fp:
        finished_partitions = pickle.load(fp)
    dfn, finished_partitions = make_l_diverse(df_dict,finished_partitions)
    save_diversity=finished_partitions.copy()
    
    print("Building anonymized Dataset \n") 
    dfn=build_anonymized_dataset(dfn,finished_partitions)
    save_builded=dfn.copy()
    
    """
    print("Generating Salt Table \n")
    dfn.id_user=dfn.id_user.astype(str)
    dfn_copy=dfn.copy()
    salt_table = generateSalts()
    salt_dict = salt_table.T.to_dict('list')

    print("Generating F Table \n")
    hashme.counter=0
    F,F_dict = generateF(df)
    months=list(range(1,14))
    month_dict={ i : months[i] for i in range(0, len(months) ) }
    F = pd.DataFrame.from_dict(F_dict,orient="index")
    F = F.rename(columns=month_dict)
    
    print("Populating F \n")
    populateF()
    hashed_copy=dfn.copy() 
    
    print("\nRegenerating F Table \n")
    months=list(range(1,14))
    month_dict={ i : months[i] for i in range(0, len(months) ) }
    F = pd.DataFrame.from_dict(F_dict,orient="index")
    F = F.rename(columns=month_dict)
    F.to_csv("F_truth_pres.csv",index=True)
    """

    print("Converting DFN and output \n")
    dfn["date"]=dfn["date"].astype("datetime64[ns]")
    dfn["qty"]=df["qty"].astype("int")
    dfn["price"]=df["price"].round(2)
    dfn["date"]=dfn["date"].apply(lambda x: x.strftime('%Y/%m/%d'))
    dfn["hours"]=dfn.apply(lambda x: hour_to_int(x["hours"]), axis=1)
    dfn.drop(["month","year"],axis=1,inplace=True)
    dfn.sort_values(["date"],inplace=True)
    dfn=retransformDel(dfn)
    dfn.to_csv("./k4nU_without_hash_l4.csv",index=False)
    dfa=pd.read_csv("./k4nU_without_hash_l4.csv")
    
    print("Checking Output format \n")
    ground_truth, submission = round1_preprocessing(
        "./ground_truth.csv", "./k4nU_without_hash_l4.csv"
    )
    check_format_trans_file(ground_truth, submission)

    print("Generating Metric Object \n")
    metric = Metrics(ground_truth, submission)
    
    print("Calculating Utility Scores \n")
    scores = metric.scores_util()
    print(scores)
    with open('scores_k4nU_without_hash_l4', 'wb') as fp:
        pickle.dump(scores, fp)
