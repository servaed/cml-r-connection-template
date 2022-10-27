#CONNECT TO CDW IMPALA / HIVE /UNIFIED ANALYTICS USING SPARK HWC


#Run this once
#install.packages("sparklyr")


#Set parameters
sparklyr_jars_default <- '/opt/spark/optional-lib/hive-warehouse-connector-assembly.jar'
hiveserver2_jdbc_url <- '<your_jdbc_url>'
workload_username <- '<your_workload_username>'
workload_password <- '<your_workload_password>'


#Combine parameters
hiveserver2_full_jdbc_url <- paste(hiveserver2_jdbc_url,';user=', workload_username, ';password=', workload_password,sep='')
hiveserver2_full_jdbc_url


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
config$spark.sql.hive.hiveserver2.jdbc.url= hiveserver2_full_jdbc_url 


# Required setting for HWC-direct reader - the hiveacid sqlextension does the automatic
# switch between reading through HWC (for managed tables) or spark-native (for external)
# depending on table type.
config$spark.sql.extensions="com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
config$spark.kryo.registrator="com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
config$sparklyr.jars.default <- sparklyr_jars_default

sc <- spark_connect(config = config)

ss <- spark_session(sc)
hive <- invoke_static(sc,"com.hortonworks.hwc.HiveWarehouseSession","session",ss)%>%invoke("build")
df <- invoke(hive,"execute","<sample_query>")
sparklyr::sdf_collect(df)