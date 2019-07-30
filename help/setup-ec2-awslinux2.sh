# 1. Set time zone - TZ and CRON_TZ
# Amazon instances setup quick quick startup are in the UTC time zone. Therefore cron jobs set to local times will not run as expected.
# list of timezone:ls /usr/share/zoneinfo
# Change the ZONE="UTC" to be ZONE="Africa/Johannesburg"
sudo sed -i '/ZONE="UTC"/c\ZONE="Africa/Johannesburg"' /etc/sysconfig/clock 
# Create symbolic link to local time
sudo ln -sf /usr/share/zoneinfo/Africa/Johannesburg /etc/localtime


# 2. Clone git repositories

# 3. Cron - start/setup

# 4. Install pip3 
sudo yum install pip3

# 5. Install virtualenv
sudo pip3 install virtualenv

# 6. Create virtual env for data job
virtualenv data-jobs




