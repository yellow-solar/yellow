# Test live whether all cron jobs are working
cd ~/yellow 

# Angaza jobs
~/.venv/data-jobs/bin/python ~/yellow/batch/angaza_pull.py                               

#Zoho upload jobs
~/.venv/data-jobs/bin/python ~/yellow/batch/zoho_upload.py Accounts_Data_Import          
~/.venv/data-jobs/bin/python ~/yellow/batch/zoho_upload.py Agents_Data_Import            
~/.venv/data-jobs/bin/python ~/yellow/batch/zoho_upload.py Applications_Credit_Details   
~/.venv/data-jobs/bin/python ~/yellow/batch/zoho_upload.py Applications_Personal_Details 
~/.venv/data-jobs/bin/python ~/yellow/batch/zoho_upload.py Payments_Data_Import          
~/.venv/data-jobs/bin/python ~/yellow/batch/zoho_upload.py Replaced_Units_Record         
~/.venv/data-jobs/bin/python ~/yellow/batch/zoho_upload.py Stock_Data_Import             
~/.venv/data-jobs/bin/python ~/yellow/batch/zoho_upload.py Dolo_Scores                   

# Yellow jobs
~/.venv/data-jobs/bin/python  ~/yellow/batch/periods_update.py                           
  