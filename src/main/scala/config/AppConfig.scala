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

  val jdbcUrl: String = config.getString("jdbc.url")
  val jdbcUsername: String = config.getString("jdbc.username")
  val jdbcPassword: String = config.getString("jdbc.password")

  val jdbcProperties = new Properties()
  jdbcProperties.setProperty("user", jdbcUsername)
  jdbcProperties.setProperty("password", jdbcPassword)
}
