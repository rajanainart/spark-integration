package com.rajanainart

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {
    val config        = new SparkConf()
    val memSqlDbKey   = String.valueOf(Common.appConfig.getJsonProperty("memsql-db-key"))
    val memSqlDbProps = Common.dbConfig.getJsonPropertiesStartsWith(memSqlDbKey)
    val host = memSqlDbProps.get(String.format("%s.datasource.host", memSqlDbKey)).get.toString
    val port = memSqlDbProps.get(String.format("%s.datasource.port", memSqlDbKey)).get.toString
    val user = memSqlDbProps.get(String.format("%s.datasource.username", memSqlDbKey)).get.toString
    val pwd  = memSqlDbProps.get(String.format("%s.datasource.password", memSqlDbKey)).get.toString
    val db   = memSqlDbProps.get(String.format("%s.datasource.database", memSqlDbKey)).get.toString

    config.set("spark.memsql.host"    , host)
    config.set("spark.memsql.port"    , port)
    config.set("spark.memsql.user"    , user)
    config.set("spark.memsql.password", pwd )
    config.set("spark.memsql.defaultDatabase"  , db)
    config.set("spark.memsql.defaultCreateMode", "Skip")

    Common.sparkSession = SparkSession
      .builder()
      //.master (String.valueOf(Common.appConfig.getJsonProperty("master"  )))
      .appName(String.valueOf(Common.appConfig.getJsonProperty("app-name")))
      .config (config)
      .getOrCreate()

    val client = new KafkaClient()
    client.consume()
  }
}
