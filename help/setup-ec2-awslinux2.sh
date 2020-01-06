# 1. Set time zone - TZ and CRON_TZ
# Amazon instances setup quick quick startup are in the UTC time zone. Therefore cron jobs set to local times will not run as expected.
# list of timezone:ls /usr/share/zoneinfo
# Change the ZONE="UTC" to be ZONE="Africa/Johannesburg"
sudo sed -i '/ZONE="UTC"/c\ZONE="Africa/Johannesburg"' /etc/sysconfig/clock 
# Create symbolic link to local time
sudo ln -sf /usr/share/zoneinfo/Africa/Johannesburg /etc/localtime

# 2. Clone git repositories
# prod jobs - master
git clone 
#beta branch
git clone
#django page 
git

# 3. Cron - start/setup
#got into project folder with cron file
crontab cronfile.txt
#check it's been read
crontab -l
#-r to remove and then you can re add

#  Install mysqlclient
sudo yum install mysql
# install files for python mysqlclient instalation
sudo yum install libssl-dev
sudo yum -y install gcc
sudo yum install python3-devel python-devel mysql-devel

#  Install pip3 
sudo yum install pip3

 # Install virtualenv
sudo pip3 install virtualenv

# Create virtual env for data job
virtualenv .venv/data-jobs

# # activate it and install requirements
# pip install -r requirements.txt

# Install g client for storage access
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

# Postgresql
sudo yum install postgresql-devel
