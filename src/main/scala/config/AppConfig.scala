package config

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

/**
  * Created by jconley on 10/3/16.
  *
  * Inspired by https://github.com/typesafehub/config/blob/master/examples/scala/simple-lib/src/main/scala/simplelib/SimpleLib.scala
  *
  * Fail fast when dealing with config values!
  */
class AppConfig(config: Config) {

  //use default config on classpath if no config is passed here
  def this(){
    this(ConfigFactory.load())
  }

//  val SparkMaster: String = config.getString("spark.master")
//  val SparkParallelismDefaultOption: String = config.getString("spark.default.parallelism")
//  val SparkSQLCaseSensitive: Boolean = config.getBoolean("spark.sql.caseSensitive")
//
//  val CassandraHost: String = config.getString("spark.cassandra.host")
//  val CassandraWriteConsistency: String = config.getString("spark.cassandra.output.consistency.level")
//  val Persist: Boolean = config.getBoolean("persist")

  val oracleUrl: String = config.getString("oracle.url")
  val oracleUsername: String = config.getString("oracle.username")
  val oraclePassword: String = config.getString("oracle.password")

  val oracleJDBCProperties = new Properties()
  oracleJDBCProperties.setProperty("user", oracleUsername)
  oracleJDBCProperties.setProperty("password", oraclePassword)

  val postgresUrl: String = config.getString("postgres.url")
  val postgresUsername: String = config.getString("postgres.username")
  val postgresPassword: String = config.getString("postgres.password")

  val postgresJDBCProperties = new Properties()
  postgresJDBCProperties.setProperty("user", postgresUsername)
  postgresJDBCProperties.setProperty("password", postgresPassword)
}
