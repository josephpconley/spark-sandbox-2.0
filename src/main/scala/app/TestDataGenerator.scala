package app

import dao.Datasource
import models.{LockerCQLTransaction, LockerTransaction}
import util.{BaseDriver, Writers}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.cassandra._

object TestDataGenerator extends BaseDriver with Writers {

  override def run = {
    import sql.implicits._

//    val numRecords = Math.pow(10, 6).toInt

    //caching a small list locally then sending work to drivers prevents the "large task size" messages
    val numRecords = sc.parallelize(0 to 50).cache()
    val transactions = numRecords.flatMap(i => LockerCQLTransaction.generate(i * 1000))
    val df = transactions.toDS().toDF()

    /** Runtimes
      * |  #     | Oracle  | Parquet | CQL
      * | 1.275M | 488.668s| 14.608s | 46.746s
      */

    parquet(df)
//    cassandra(df)

    /**
      * Query times - Group by
      *
      * |  #     | Oracle (s) | Parquet (s)
      * | 1.275M | 12.266     | 0.472
      *
      */

    /**
      * Upsell queries (served by Spring Boot)
      *
      * |  #     | Oracle (s) | CQL (s)
      * | 1.275M | 21.5     | 18.2
      *
      */
  }

  private def oracle(df: DataFrame): Unit ={
    Datasource.execute("delete from locker_transactions")
    df.write.mode(SaveMode.Append).jdbc(appConfig.jdbcUrl, "locker_transactions", appConfig.jdbcProperties)
  }

  private def parquet(df: DataFrame): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet("/home/jconley/data/transactions.parquet")
  }

  private def cassandra(df: DataFrame) = {
    df.write.mode(SaveMode.Append).cassandraFormat("locker_test", "upsell").save()
  }
}
