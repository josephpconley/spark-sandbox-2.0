package app

import dao.Datasource
import models.{LockerCQLTransaction, LockerTransaction}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, SaveMode}
import util.{BaseDriver, Writers}

object TestDataGenerator extends BaseDriver with Writers {

  override def run = {
    import sql.implicits._

    //TODO figure out why this takes too longs/fails
//    val numRecords = Math.pow(10, 6).toInt
//    val transactions = sc.parallelize(LockerTransaction.generate(numRecords))

    //caching a small list locally then sending work to drivers prevents the "large task size" messages
    //like factorial but uses addition (triangle)
    /**
      * 1 to 50 - 1M records
      * 1 to 89 - 4M records
      */

    val numRecords = sc.parallelize(1 to 89).cache()
    val transactions = numRecords.flatMap(i => LockerTransaction.generate(i * 1000, 24))
    val df = transactions.toDS().toDF()

    /** Runtimes
      * |  #     | Oracle  | Parquet | CQL      | Postgres
      * | 1.276M | 488.668s| 14.608s |  46.746s |
      * | 4.006M |3590.888s| failed  | 148.164s | 111.733s
      */

//    parquet(df)
//    cassandra(df)
//    oracle(df)
    postgres(df)

    /**
      * Query times - Group by (cold)
      *
      * |  #     | Oracle  | Parquet
      * | 1.276M | 12.266s | 0.472s
      * | 4.006M |         |
      */

    /**
      * Transactions queries (served by Spring Boot)
      *
      * |  #                | Oracle (s)       | CQL (s) | Presto (s)
      * | 1.276M (all data) | 21.5             | 18.2
      * | 4.006M (3 months) | 294.764 (505825) | 10.436 (514470) | 28.868 (509295)
      *
      */
  }

  private def oracle(df: DataFrame): Unit ={
    Datasource.execute("delete from locker_transactions")
    df.write.mode(SaveMode.Append).jdbc(appConfig.oracleUrl, "locker_transactions", appConfig.oracleJDBCProperties)
  }

  private def parquet(df: DataFrame): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet("/home/jconley/data/transactions.parquet")
  }

  private def cassandra(df: DataFrame) = {
    df.write.mode(SaveMode.Append).cassandraFormat("locker_test", "upsell").save()
  }

  private def postgres(df: DataFrame) = {
    Class.forName("org.postgresql.Driver")
    df.write.mode(SaveMode.Append).jdbc(appConfig.postgresUrl, "locker_transactions", appConfig.postgresJDBCProperties)
  }
}
