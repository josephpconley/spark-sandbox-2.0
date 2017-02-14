package util

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.cassandra._

trait Writers {

  def writeToCassandra(df: DataFrame, schemaName: String, tableName: String) =
    df.write.mode(SaveMode.Append).cassandraFormat(tableName, schemaName).save()

  def writeToCSV(df: DataFrame, folderName: String) =
    df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(folderName)
}
