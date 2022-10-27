#CONNECT TO DATA LAKE USING SPARK HWC


#Run this once
#install.packages("sparklyr")


#Set parameters
sparklyr_jars_default <- "/opt/spark/optional-lib/hive-warehouse-connector-assembly.jar"


#Add library
library(sparklyr)


#Add spark_config()
spark_config()
config <- spark_config()


#Add configuration
config$spark.security.credentials.hiveserver2.enabled="false"
config$spark.datasource.hive.warehouse.read.via.llap="false"
config$spark.sql.hive.hwc.execution.mode="spark"
#config$spark.datasource.hive.warehouse.read.jdbc.mode="spark"


# Required setting for HWC-direct reader - the hiveacid sqlextension does the automatic
# switch between reading through HWC (for managed tables) or spark-native (for external)
# depending on table type.
config$spark.sql.extensions="com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
config$spark.kryo.registrator="com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
config$sparklyr.jars.default <- sparklyr_jars_default

#Connect with spark
sc <- spark_connect(config = config)
src_databases(sc)


#Change database if necessary
spark_session(sc) %>% invoke("sql", "USE <sample_database>")


#Read and show table
intDf1 <- sparklyr::spark_read_table(sc, '<sample_table>')
sparklyr::sdf_collect(intDf1)