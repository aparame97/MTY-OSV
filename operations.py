import time
import pandas as pd
import prestodb
import pymssql
from datetime import datetime, date
import sys

import dbops as dbo



def NodeDateLinkage(df,ludp_conn):
     
    flex_conn,flex_cursor = dbo.flex_connect("s_sc_gsc","Initial0","10.99.244.203:1458")

    print("\nGetting Original Failure dates\n")
    time.sleep(0.2)
    print("\nLinking Parts to the Nodes\n")
    sn = list(df['Supplier SN'].unique())
    
    #Construct the INSERT statement to insert the data into a temp table #TempData

    query = "SELECT serial_number,old_barcode,stamp FROM `proj_mi`.`msft_part_replace` where plant in ('X470','X270') and stamp >= cast(date_add('month', -4, date_add('day', 1 - day(current_date),  current_date)) as VARCHAR) and old_barcode in ( "

    for i in range(0,len(sn)-1):
        query += f"'{sn[i]}'," #add all values upto last value
        
    query += f"'{sn[-1]}')"

    origfails = pd.read_sql(query,ludp_conn)
    origfails.rename(columns={'old_barcode':'Supplier SN'},inplace=True)
    origfails['Failure Date'] = pd.to_datetime(origfails['stamp'].astype(str))
    origfails['Fail Date'] = pd.to_datetime(origfails['stamp'].astype(str)).dt.date
    origcols = ['Supplier SN','Failure Date']
    origfails.insert(0,"SN_Stamp",origfails[origcols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1))
    origfails = origfails[['Supplier SN','Failure Date','Fail Date','serial_number']] 

    df = df.sort_values(by="Failure Date")
    origfails = origfails.sort_values(by="Failure Date")

    res = pd.merge_asof(df,origfails,on='Failure Date',by='Supplier SN',direction='nearest')
    res.dropna(subset = 'serial_number',inplace = True)
    res.drop(['Failure Date'],axis = 1,inplace=True)

    column_to_move = res.pop("serial_number")
    res.insert(2, "Node SN", column_to_move)

    column_to_move = res.pop("Fail Date")
    res.insert(3, "Fail Date", column_to_move)

    res.insert(3, "Site", "MTY")
    res.insert(0,'Report Date',str(date.today()))

    res_pfa = res[res['Status'] == "PFA"]
    res_pfa.reset_index(drop=True,inplace=True)

    if res_pfa.shape[0] > 0:
        try:
            dbo.osv_push(res_pfa,"[DM_PCEBG_BIZ].[SC].[L1FA_PendingResultData]",flex_conn,flex_cursor)
            print("\nSuccesfully Uploaded MTY PFA Data!")
            
        except Exception as err:
            err_log = open(f"ErrorLog_{str(datetime.now().isoformat(sep="_", timespec="seconds")).replace(":", "-")}.txt", "a")
            err_log.write(f"ERROR: operations.py -> PFA Data Push {str(err)})")
            err_log.close() 
            sys.exit()
    
    res = res[res['Status'] != "PFA"]
    res.reset_index(drop=True,inplace=True)

    print(res.shape)

    print("\nPushing Data to SQL..\n")
    try:
        dbo.osv_push(res,"[DM_PCEBG_BIZ].[SC].[L1FA_ResultData]",flex_conn,flex_cursor)
        print("\nSuccesfully Uploaded MTY Data!")
        
    except Exception as err:
        err_log = open(f"ErrorLog_{str(datetime.now().isoformat(sep="_", timespec="seconds")).replace(":", "-")}.txt", "a")
        err_log.write(f"ERROR: operations.py -> OSV Data Push {str(err)})")
        err_log.close() 
        sys.exit()


def Cleaner(df,ludp_user,ludp_pass):

    ludp_conn = dbo.ludp_connect(ludp_user,ludp_pass)
     
    print("\nProcessing and cleaning the input data..\n")
    status = ['VID', 'NDF', 'CID','PFA']

    df = df[~df['Supplier SN'].isna()]
    df = df[~df['Failure Date'].isna()]
    df['Failure Date'] = pd.to_datetime(df['Failure Date'].astype(str))
    df['Status'] = df['Status'].str.strip().str.upper().replace({"RTV":"VID","SCRAP":"CID","Pending for Analysis":"PFA"})
    df=df[df['Status'].isin(status)]
    df = df[['Supplier SN','Failure Date','Status']]

    cols = ['Supplier SN','Failure Date']
    df.insert(0,"SN_Stamp",df[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1))
    
    
    NodeDateLinkage(df,ludp_conn)