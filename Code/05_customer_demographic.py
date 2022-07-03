from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import xml.etree.ElementTree as ET

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Customer Demographics extraction")\
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    hive_df = spark.sql("select * from testdb.customer_test")
    projDF = hive_df.select("CustomerID", "Demographics")
    cols = ["TotalPurchaseYTD", "DateFirstPurchase", "BirthDate", "MaritalStatus", "YearlyIncome", "Gender",
            "TotalChildren", "NumberChildrenAtHome", "Education", "Occupation", "HomeOwnerFlag", "NumberCarsOwned",
            "CommuteDistance"]
    str1 = '{http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey}'

    rdd = projDF.rdd.map(lambda x: [x[0]]+[ET.fromstring(x[1]).find(str1+i).text for i in cols ])
    df = spark.createDataFrame(rdd,["CustomerID"]+cols)
    df.show(5)
    df.printSchema()
    df.write.saveAsTable("testdb.customer_demographics")





    #value = ET.fromstring(projDF[]).find('response/result/value')
