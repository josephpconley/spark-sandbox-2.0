package app

import models.LockerTransaction
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu._
import util.{BaseDriver, Writers}

import scala.collection.JavaConversions._

object KuduTestDataGenerator extends BaseDriver with Writers {

  val kuduContext = new KuduContext("quickstart.cloudera:7051")
  val tableOptions = new CreateTableOptions().setNumReplicas(1).setRangePartitionColumns(Seq("TRANSACTION_ID"))

  override def run = {
    //caching a small list locally then sending work to drivers prevents the "large task size" messages
    write

//    read
    
  }

  def read = {
    val df = sql.read.options(Map("kudu.master" -> "quickstart.cloudera","kudu.table" -> "sfmta")).kudu
    df.printSchema()
  }

  def write = {
    import sql.implicits._

    val numRecords = sc.parallelize(0 to 10).cache()
    val transactions = numRecords.flatMap(i => LockerTransaction.generate(i * 100))
    val df = transactions.toDS().toDF()

    kuduContext.createTable("locker_transactions", df.schema, Seq("TRANSACTION_ID"), tableOptions)
    kuduContext.insertRows(df, "locker_transactions")
  }
}
