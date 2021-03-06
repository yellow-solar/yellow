### Config

# delete crontab
crontab -r

# add new crontab 
crontab config/cron_setup.txt

# list crontab to check
crontab -l

# Copy the .bash_profile
cp config/.bash_profile ~/.bash_profile  

# Update venv for data-jobs
source ~/.venv/data-jobs/bin/activate
#make sure you installed mysql pre-reqs
pip install -r requirements.txt
deactivate

# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib



