package app.cql

import com.datastax.driver.core.SimpleStatement
import com.datastax.spark.connector.cql.CassandraConnector
import util.BaseDriver

object CQLTransactions extends BaseDriver {

  override def run(): Unit = {
    val transactions = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/jconley/Desktop/to-delete.csv")

    transactions.collect().foreach { row =>
      val transactionId = row.getAs[String]("transaction_id")
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        val stmt = new SimpleStatement(s"delete from upsell.locker_transactions where transaction_id = '$transactionId'")
        stmt.setIdempotent(true)
        session.executeAsync(stmt)
      }
    }
  }
}
