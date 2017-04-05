package dao

import java.sql.{Connection, Timestamp}

import anorm._
import com.zaxxer.hikari.HikariDataSource
import config.AppConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DecimalType, DoubleType, LongType, TimestampType}

/**
  * Created by jconley on 10/3/16.
  */
object Datasource {

  private val appConfig = new AppConfig()

  private val ds = new HikariDataSource()
  ds.setJdbcUrl(appConfig.oracleUrl)
  ds.setUsername(appConfig.oracleUsername)
  ds.setPassword(appConfig.oraclePassword)

  def withConnection[A](block: Connection => A): A = {
    val connection: Connection = ds.getConnection

    try {
      block(connection)
    } finally {
      connection.close()
    }
  }

  def insert(row: Row, tableName: String): Unit = withConnection { implicit c =>
    val fields = row.schema.fields.toList
    val sql =
      s"""
         |insert into $tableName (${fields.map(_.name).mkString(", ")})
         |values (${fields.map(f => "{" + f.name + "}").mkString(", ")})
       """.stripMargin

    println(sql)

    val params: Seq[NamedParameter] = fields.map { field =>
      val param: NamedParameter = field.dataType match {
        case LongType => field.name -> row.getAs[Long](field.name)
        case TimestampType => field.name -> row.getAs[Timestamp](field.name)
        case DecimalType() => field.name -> row.getAs[java.math.BigDecimal](field.name)
        case DoubleType => field.name -> row.getAs[Double](field.name)
        case _ => field.name -> row.getAs[String](field.name)
      }
      param
    }

    try {
      SQL(sql).on(params:_*).execute()
    } catch {
      case e: Exception =>
        println(row)
        throw e
    }
  }

  def execute(sql: String): Unit = withConnection { implicit c =>
    SQL(sql).execute()
  }
}