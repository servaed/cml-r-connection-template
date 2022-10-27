#CONNECT TO CDW IMPALA / HIVE /UNIFIED ANALYTICS USING JDBC

#Git for JDBC driver file
#https://github.com/servaed/unified-analytics-jdbc.git

#Run this once
#install.packages("DBI")
#install.packages("rJava")
#install.packages("RJDBC")

#Set parameters
hive_jdbc_driver <- '/home/cdsw/hive-jdbc-3.1.0-SNAPSHOT-standalone.jar'
hiveserver2_jdbc_url <- '<your_jdbc_url>'
workload_username <- '<your_workload_username>'
workload_password <- '<your_workload_password>'

#Add Library
library("DBI")
library("rJava")
library("RJDBC")



#Initialize Connection
drv <- JDBC("org.apache.hive.jdbc.HiveDriver", hive_jdbc_driver, identifier.quote="`")

conn <- dbConnect(drv,hiveserver2_jdbc_url, workload_username, workload_password)

#working with the connection
show_databases <- dbGetQuery(conn, "show databases")
show_databases

select_query <- dbGetQuery(conn, "<sample_query>")
select_query