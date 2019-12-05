package com.rajanainart

import org.apache.spark.sql.SparkSession

object Common {
  var sparkSession : SparkSession = null
  var appConfig    : JsonConfig   = new JsonConfig("app-config.json", 1)
  val appEnv       : String       = String.valueOf(appConfig.getJsonProperty("app-env"))
  var kafkaConfig  : JsonConfig   = new JsonConfig(String.format("kafka-config-%s.json", appEnv), 1)
  var dbConfig     : JsonConfig   = new JsonConfig(String.format("db-config-%s.json"   , appEnv), 1)
  var urlConfig    : JsonConfig   = new JsonConfig(String.format("url-config-%s.json", appEnv), 1)
  val cosaUrl      : String       = String.valueOf(urlConfig.getJsonProperty("cosa.url"))

  val DB_DRIVER_KEY    = "%s.datasource.driver-class-name"
  val DB_USER_NAME_KEY = "%s.datasource.username"
  val DB_PASSWORD_KEY  = "%s.datasource.password"
  val DB_JDBC_URL      = "%s.datasource.url"
  val DB_SCHEMA_KEY    = "%s.datasource.database"

  private var memSqlQuery : String = ""

  def clearCache(id : String): Unit = {
    for ((k,v) <- Common.sparkSession.sparkContext.getPersistentRDDs) {
      Common.log(id, "Clearing RDD:"+k.toString)
      v.unpersist()
    }
    if (Common.sparkSession.sqlContext != null) {
      Common.log(id, "Clearing SQL Context Cache")
      Common.sparkSession.sqlContext.clearCache()
    }
  }

  def log(id : String, msg : String) = {
    println(msg)
    val query : String = String.format(
      """
         INSERT INTO CMN_INTEGRATION_PROCESS_LOG (
            process_id, log, as_on
         )
         SELECT %s, '%s' AS msg, CURRENT_TIMESTAMP
        """, id, msg).stripMargin

    val db = new Database("local")
    db.executeQuery(query)
  }

  def buildDeleteQuery(idx : Integer, jsonParser: JsonConfig) : String = {
    var count      = 0
    val parameters = jsonParser.getJsonPropertiesStartsWith(String.format("%s-parameter-", idx))
    val query      = new StringBuilder
    query.append(String.format("DELETE FROM %s WHERE 1 = 1", jsonParser.getJsonProperty(String.format("%s-target-table", idx))))

    for (field <- jsonParser.getJsonPropertiesStartsWith(String.format("%s-field-", idx))) {
      val f      = field._2.asInstanceOf[Map[String, Object]]
      val target = f.get("targetField").get.toString
      val fType  = f.get("type"       ).get.toString
      val isPk   = Boolean.unbox(f.get("isPk").get)
      val key    = "parameter-"+target

      if (isPk && parameters.contains(key)) {
        if (fType == "INTEGER" || fType == "NUMERIC")
          query.append(String.format(" AND %s = %s"  , target, parameters.get(key).get))
        else if (fType == "TEXT")
          query.append(String.format(" AND %s = '%s'", target, parameters.get(key).get))
        else if (fType == "DATE")
          query.append(String.format(" AND %s = TO_DATE('%s', '%s')"  , target, parameters.get(key).get, parameters.get("format").get))

        count = count+1
      }
    }
    if (count == 0)
      throw new IllegalArgumentException("No conditions found for deleting the rows")

    query.toString()
  }

  def setMemSqlUpdateQuery(idx : Integer, jsonParser : JsonConfig) {
    var query  : String = String.format("""
       INSERT INTO %s (COLUMN_LIST)
       SELECT SELECT_LIST
       ON DUPLICATE KEY UPDATE
       UPDATE_LIST""", jsonParser.getJsonProperty(String.format("%s-target-table", idx))).stripMargin

    val column : StringBuilder = new StringBuilder
    val select : StringBuilder = new StringBuilder
    val update : StringBuilder = new StringBuilder
    var counter: Int = 0

    for (field <- jsonParser.getJsonPropertiesStartsWith(String.format("%s-field-", idx))) {
      val f        = field._2.asInstanceOf[Map[String, Object]]
      val target   = f.get("targetField").get.toString
      val autoIncr = Boolean.unbox(f.get("autoIncr").get)

      if (!autoIncr) {
        column.append(String.format(" %s%s"     , if (counter != 0) "," else "", target))
        select.append(String.format(" %s? AS %s", if (counter != 0) "," else "", target))
        update.append(String.format(" %s%s = VALUES(%s)", if (counter != 0) "," else "", target, target))

        counter = counter + 1
      }
    }
    query = query.replaceAll("COLUMN_LIST", column.toString)
    query = query.replaceAll("SELECT_LIST", select.toString)
    query = query.replaceAll("UPDATE_LIST", update.toString)

    memSqlQuery = query
  }

  def getMemSqlBaseUpdateQuery(): String = {
    memSqlQuery
  }
}
