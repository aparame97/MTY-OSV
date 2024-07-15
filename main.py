
import pandas as pd
import glob
import warnings
from datetime import datetime

from operations import Cleaner
from housekeeping import pretty_print


warnings.filterwarnings('ignore')

print("\n\n")
pretty_print("MTY OSV Tool")
print("\n**********************************************************************************")
print("\n********* built with <3 by Adarsh Parameswaran (aparameswara@lenovo.com) *********\n\n")
print("Please ensure the following before you proceed\n")
print("[1]. You are either on the Lenovo network or using the VPN")
print("[2]. The file has the following columns with the specific header : 'Supplier SN','Failure Date','Status' ; in any order")
print("[3]. You have access to the 'msft_part_replace' table in LUDP")

input("\n\nPlease enter any key to continue \t")


print("\nProgram Run Start\n")
#Import initial fails

ludp_user = str(input("\n\nEnter username : "))
ludp_pass = str(input("\nEnter password: "))




files = glob.glob('*.xls*')

print("\n\nChoose the file to upload\n\n")

for i in range(0,len(files)):
     print(f"[{i+1}] : {files[i]}\n")
           
selection = (input("\nEnter your Selection : \t"))

try:
    index = int(selection) - 1
    if 0 <= index < len(files):
         df = pd.read_excel(files[index])
         Cleaner(df,ludp_user,ludp_pass)
         input("\nRun Complete. Program terminating. Press any key to end\t")
    
    else:
         print("Error: Invalid selection.")
        
except ValueError:
    print("Error: Invalid input.")
    
