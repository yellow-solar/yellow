### Config
rm -r yellow

git clone

# delete crontab
crontab -r

# add new crontab 
crontab tools/cron_setup.txt

# list crontab to check
crontab -l

# Copy the .bash_profile
cp .bash_profile ~/.bash_profile  

# Update venv for data-jobs
source .venv/data-jobs/bin/activate
pip install -r requirements
deactivate


