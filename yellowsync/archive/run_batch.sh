# Test live whether all cron jobs are working
cd ~/yellow 

# Angaza jobs
~/.venv/data-jobs/bin/python ~/yellow/yellowsync/angazapull.py                               

#Zoho upload jobs
~/.venv/data-jobs/bin/python ~/yellow/yellowsync/zohoupload.py Accounts_Data_Import          
~/.venv/data-jobs/bin/python ~/yellow/yellowsync/zohoupload.py Agents_Data_Import            
~/.venv/data-jobs/bin/python ~/yellow/yellowsync/zohoupload.py Applications_Credit_Details   
~/.venv/data-jobs/bin/python ~/yellow/yellowsync/zohoupload.py Applications_Personal_Details 
~/.venv/data-jobs/bin/python ~/yellow/yellowsync/zohoupload.py Payments_Data_Import          
~/.venv/data-jobs/bin/python ~/yellow/yellowsync/zohoupload.py Replaced_Units_Record         
~/.venv/data-jobs/bin/python ~/yellow/yellowsync/zohoupload.py Stock_Data_Import             
~/.venv/data-jobs/bin/python ~/yellow/yellowsync/zohoupload.py Dolo_Scores                   

# Yellow jobs
~/.venv/data-jobs/bin/python  ~/yellow/yellowsync/periodsupdate.py  

# Airtel statements
~/.venv/data-jobs/bin/python -m statements.mobilestatements
  