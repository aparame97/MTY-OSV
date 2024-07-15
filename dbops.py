## Contains all housekeeping code to connect and push data to various databases 
## Adarsh Parameswaran - Product Engineer - July 2024

import prestodb
import pymssql
from datetime import datetime, date
import sys


# Function to push data into a flex database
def osv_push(dfx,tablename,flex_conn,flex_cursor):

    #Upload to Flex

    # Create Temp Table
    create_temp_query = f"IF OBJECT_ID('tempdb.dbo.#TempData', 'U') IS NOT NULL DROP TABLE #TempData  Create table #TempData ([REPORT_DATE] date,[SN_Stamp]  varchar(max),[Part SN] varchar(max),[NodeSN] varchar(max),[Site]  varchar(20),[Failure Found Date] date,[L1 FA]  varchar(max))"
    flex_cursor.execute(create_temp_query)

    #Construct the INSERT statement to insert the data into a temp table #TempData

    query = "INSERT into #TempData "

    firstrow = list(dfx.loc[0].values) #get first row
    query += f"select '{firstrow[0]}'" #add first value of first row
    
    for k in range(1,len(firstrow)): #selects remaining values in first row and adds
            query += f",'{firstrow[k]}'"

    for i in range(1,dfx.shape[0]): #selects each subsequent row
        row_values = list(dfx.loc[i].values)
        query += f" union all select '{row_values[0]}'"

        for j in range(1,len(row_values)): #selects each value in tje current column
            query += f",'{row_values[j]}'"
 
    try:
         
        flex_cursor.execute(query)
        flex_conn.commit()

        # Construct the commit query to check for existing data in the database and
        # commit new data to [DM_PCEBG_BIZ].[SC].[L1FA_ResultData]

        commit_query = f"delete #TempData where #TempData.[SN_Stamp] in (select [SN_Stamp] from {tablename}) insert into {tablename} select * from #TempData "
        flex_cursor.execute(commit_query)
        flex_conn.commit()


    except:
        err_log = open(f"ErrorLog_{str(datetime.now().isoformat(sep="_", timespec="seconds")).replace(":", "-")}.txt", "a")
        err_log.write("ERROR: dbops.py -> osv_push()")
        err_log.close()  
        sys.exit()


# Function to instantiate a connection to LUDP 
def ludp_connect(username,password):
    
    conn = prestodb.dbapi.connect(
        host='presto.dbc.ludp.lenovo.com', #(TST environment: dev-presto.ludp.lenovo.com)
        port=30060, #(TST environment: 30011)
        user=username,  # Input your data account name
        catalog='hive',
        http_scheme='https',
        auth=prestodb.auth.BasicAuthentication(username, password),  # Input your data account &amp; password
    )
    #conn._http_session.verify = "C:/Presto/presto.cer" # Input your presto certification file path
    conn._http_session.verify = False

    return(conn)


# Function to instantiate a connection to Flex Reporting

def flex_connect(username,password,server_ip):

    try:
         conn = pymssql.connect(server_ip, username, password, "tempdb")
         cursor = conn.cursor(as_dict = True)
    
    except:
         err_log = open(f"ErrorLog_{str(datetime.now().isoformat(sep="_", timespec="seconds")).replace(":", "-")}.txt", "a")
         err_log.write("ERROR: dbops.py -> Flex connection failed. Check credentials or ensure VPN is being used if not on the office network")
         err_log.close()  
         sys.exit()

    return(conn,cursor)