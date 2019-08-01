# How to connect to the AWS RDS - MySQL database
`mysql -h <hostname> -P 3306 -u <user> -p`

#####WARNING:current settings on AWS allow connections from anywhere. Need to learn how to only connect from your pc, and with other user logins - i.e. configure data access and make sure logs are showing user history 

We can create different users with different levels of database access - but for now, will just use root

Install the workbench
`sudo apt-get install mysql-workbench-community`

Commands to select databases, tables etc.
`show databases;`
`use <db_name>;`
`show tables;`

To run a sql file:
`source fil_name.sql`

####NB: DELETING RECORDS IN DATABASES IS PERMANENT

# Helpful performance tips
### 0. InnoDB indexes - https://dev.mysql.com/doc/refman/8.0/en/mysql-indexes.html
To tune queries for InnoDB tables, create an appropriate set of indexes on each table
 - Because each InnoDB table has a primary key (whether you request one or not), specify a set of primary key columns for each table, columns that are used in the most important and time-critical queries.
 -  If you have many queries for the same table, testing different combinations of columns, try to create a small number of concatenated indexes rather than a large number of single-column indexes. If an index contains all the columns needed for the result set (known as a covering index), the query might be able to avoid reading the table data at all.
  - You can optimize single-query transactions for InnoDB tables, using the technique in Section 8.5.3, “Optimizing InnoDB Read-Only Transactions”.

### 1. Use EXISTS clause wherever needed (faster than select > 0)
`If EXISTS(SELECT * from Table WHERE col=’some value’)`

### 2. An EXPLAIN query results in showing you which indexes are being utilized, how the table is being scanned, sorted, etc.
`EXPLAIN SELECT`

### 3. Index and use the same column types for joins
Another vital tip of MySQL best practices – if your application has many JOIN queries, make sure that the columns you join by are indexed on both tables. This affects the internal optimization of the join operation by MySQL.

Also, the joined columns need to be the same type. For example, if you join a DECIMAL column to an INT column from another table, MySQL won’t be able to use any of the indexes. Even the character encodings need to be the same type for string type columns.

// looking for companies in my state
	$r = mysql_query("SELECT company_name FROM users
	LEFT JOIN companies ON (users.state = companies.state)
	WHERE users.id = $user_id");

// both state columns should be indexed
// and they both should be the same type and character encoding
// or MySQL might do full table scans

### 4. MySQL stack_trace can be used to isolate various bugs. 

### 5. MySQL Tuner is a Perl script that can somehow optimize your performance by suggesting changes to your configuration files.

# Advanced usage
3.3.1 START TRANSACTION, COMMIT, and ROLLBACK Syntax
 - start in read onlyto reduce overhead

# CONFIGURATION AND ADMINISTRATION
###NOTE: Major factor in the database is machine space, RAM and cores and clustering

Access the the INNO settings
`SHOW VARIABLES LIKE 'inno%';`

To find slow queries - see https://www.liquidweb.com/kb/mysql-performance-identifying-long-queries/
`mysqladmin proc stat`
`show processlist; > add the 'full' modifier`

Enable slow query logging 
	slow_query_log		enable/disable the slow query log
	slow_query_log_file	name and path of the slow query log file
	long_query_time		time in seconds/microseconds defining a slow query
Then dump the output after it's been logging for a while
	`mysqldumpslow`

https://dba.stackexchange.com/questions/75091/why-are-simple-selects-on-innodb-100x-slower-than-on-myisam

Other settings from my.cnf

You should increase threading

	`innodb_read_io_threads = 64`
	`innodb_write_io_threads = 16`
	`innodb_log_buffer_size = 256M`

Why query_cache_type is disabled by default start from MySQL 5.6
`query_cache_size = 0`

I would preserve the Buffer Pool:
	`innodb_buffer_pool_dump_at_shutdown=1`
	`innodb_buffer_pool_load_at_startup=1`

Increase purge threads (if you do DML on multiple tables)

	`innodb_purge_threads = 4`

https://dba.stackexchange.com/questions/66774/why-query-cache-type-is-disabled-by-default-start-from-mysql-5-6/66796#66796


### Manual
Installing and removing mysql installation from server

1. Install and initiate
 - see online how to add package to repo, then install with the following 
`sudo apt-get install mysql-server`
*_remember the password please..._*

Start
`sudo service mysql start -p`
-p option is for password and will need to be entered the first time interacting on that n that terminal
`sudo service mysql status`

End
`sudo service mysql stop`

Setup mysql to boot on startup
`systemctl enable mysqld.service`

2. Remove and re install
	`sudo apt purge mysql-server mysql-client mysql-common`
	`sudo apt autoremove`
	`sudo mv -iv /var/lib/mysql /var/tmp/mysql-backup`
	`sudo rm -rf /var/lib/mysql*`
