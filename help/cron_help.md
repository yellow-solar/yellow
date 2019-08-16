# Cron Help and Tutorial

Cron is the linux/unix scehduler. It has a *very simple interface where each job is just a line on a file:

You can view the file using:
`crontab -l`

Edit the file using (opens in your default text editor):
`crontab -e`

Change your default text editior with the following in your terminal:
`export EDITOR=nano`

You can also load a file to cron:
`crontab cron.txt`

You can add comments with  the # character.
`# This cron job does something very important`

The structure of the cron jobs looks like this:
    Timing		Execute PHP	Path to script				Output
    * * * * *	/usr/bin/php	/var/www/html/crontest/cron.php		> /dev/null 2>&1

Time five time-and-date fields are as follows: 
 - minute (0-59), 
 - hour (0-23, 0 = midnight), 
 - day (1-31), 
 - month (1-12), 
 - weekday (0-6, 0 = Sunday)

 - * = (wildcard) all
 - /x = x (e.g. 2/5 in the minutes will run every 5 mins starting at minute 2) 


Special words:
@reboot
Run once, at startup.

@yearly
Run once a year, "0 0 1 1 *".

@annually
(same as @yearly)

@monthly
Run once a month, "0 0 1 * *".

@weekly
Run once a week, "0 0 * * 0".

@daily
Run once a day, "0 0 * * *".

@midnight
(same as @daily)

@hourly
Run once an hour, "0 * * * *".


##Logs:
By default installation the cron jobs get logged to a file called /var/log/syslog
Search through using grep (text search function in linux)

`sudo grep -i cron /var/log/syslog`

## AWS EC2 LINUX-2
###install
    yum -y install crontabs
    chkconfig crond on
    service crond start
    service crond status
    
#log is found here: (because the unix is slightly different):
`sudo vim /var/log/cron`
# search for a specific thread of jobs
`sudo grep -i <keyword> /var/log/cron`


###TO DO:
 - learn about file locking to create job dependencies


 ####More help:
https://help.ubuntu.com/community/CronHowto





