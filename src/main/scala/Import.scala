package com.rajanainart

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

import scala.util.control.Breaks
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.entity.StringEntity

import scala.collection.mutable

class Import (meta : String) {
  var jsonParser  : JsonConfig = new JsonConfig(meta, 2)
  var sourceDbKey : String     = ""
  var targetDbKey : String     = ""
  var idKey       : String     = ""
  var sourceDbProperties : mutable.Map[String, Object] = null
  var targetDbProperties : mutable.Map[String, Object] = null

  /* AbstractMethodError with spark-submit
  Common.setMemSqlUpdateQuery(jsonParser)
  sourceData.write
            .format("com.rajanainart")
            .save()*/

  /*Issues with the privileges SHOW_METADATA, CLUSTER
  sourceData.write
            .format("com.memsql.spark.connector")
            .mode("overwrite")
            .save(table)*/

  def getSchema(idx : Integer) : StructType = {
    val schema  = new StructType
    for (field <- jsonParser.getJsonPropertiesStartsWith(String.format("%s-field-", idx))) {
      val loop = new Breaks
      loop.breakable {
        val f        = field._2.asInstanceOf[Map[String, Object]]
        val dataType = f.get("type"       ).get.toString
        val target   = f.get("targetField").get.toString
        val autoIncr = Boolean.unbox(f.get("autoIncr").get)

        if (autoIncr) loop.break()
        dataType match {
          case "TEXT"    => schema.add(StructField(target, StringType ))
          case "INTEGER" => schema.add(StructField(target, IntegerType))
          case "NUMERIC" => schema.add(StructField(target, DoubleType ))
          case "DATE"    => schema.add(StructField(target, DateType   ))
        }
      }
    }
    schema
  }

  def updateMetaInfo(idx : Integer): Boolean = {
    sourceDbKey = String.valueOf(jsonParser.getJsonProperty(String.format("%s-source", idx)))
    targetDbKey = String.valueOf(jsonParser.getJsonProperty(String.format("%s-target", idx)))
    if (!sourceDbKey.equalsIgnoreCase("null") && !targetDbKey.equalsIgnoreCase("null") &&
         sourceDbKey != null && targetDbKey != null && !sourceDbKey.isEmpty && !targetDbKey.isEmpty) {
      sourceDbProperties = Common.dbConfig.getJsonPropertiesStartsWith(sourceDbKey)
      targetDbProperties = Common.dbConfig.getJsonPropertiesStartsWith(targetDbKey)
      if (idx == 0) idKey = jsonParser.getJsonProperty(String.format("%s-id", idx)).toString

      println(String.format("SourceDbKey-%s:%s", idx, sourceDbKey))
      println(String.format("TargetDbKey-%s:%s", idx, targetDbKey))
      return true
    }
    false
  }

  def start(): Unit = {
    var index = 0
    val loop  = new Breaks
    var db : Database = null;

    loop.breakable {
      while (true) {
        val found = updateMetaInfo(index)
        if (!found) loop.break()

        if (index == 0) {
          Common.log(getProcessId(), "Spark process started, process-id:"+getProcessId())
          db = new Database(targetDbKey)
        }
        executePreTasks (index, db)
        executeImport   (index, db)
        executePostTasks(index, db)

        //Common.clearCache(getProcessId())
        index = index + 1
      }
    }
    if (db != null) db.close()

    callDeltaCalc()
    Common.log(getProcessId(), "Successfully completed loading data from source to target")
  }

  def executeImport(idx : Integer, db : Database): Unit = {
    Common.log(getProcessId(), String.format("Building the query:%s", idx))
    val query = buildUpdatedQuery(idx)
    println("Final Query:"+query)
    Common.log(getProcessId(), String.format("Fetching records from source:%s", idx))
    val sourceData = Common.sparkSession.read
      .format("jdbc")
      //.schema(getSchema())
      .option("driver"  , sourceDbProperties.get(String.format(Common.DB_DRIVER_KEY   , sourceDbKey)).get.toString)
      .option("url"     , sourceDbProperties.get(String.format(Common.DB_JDBC_URL     , sourceDbKey)).get.toString)
      .option("user"    , sourceDbProperties.get(String.format(Common.DB_USER_NAME_KEY, sourceDbKey)).get.toString)
      .option("password", sourceDbProperties.get(String.format(Common.DB_PASSWORD_KEY , sourceDbKey)).get.toString)
      .option("dbtable" , String.format("(%s) src1", query))
      .load()//.cache()
    Common.log(getProcessId(), "Completed fetching records: "+idx+", total records: "+sourceData.count())

    if (sourceData.count() == 0) {
      Common.log(getProcessId(), String.format("No records fetched, exiting: %s", idx))
      return
    }

    Common.log(getProcessId(), String.format("Loading records to the target: %s", idx))
    if (Database.getDatabaseType(targetDbKey) == Database.MEMSQL) {
      val table : String = String.format("%s.%s", targetDbProperties.get(String.format(Common.DB_SCHEMA_KEY, targetDbKey)).get.toString,
                                          jsonParser.getJsonProperty(String.format("%s-target-table", idx)).toString)
      sourceData.write
        .format("jdbc")
        .option("driver"  , targetDbProperties.get(String.format(Common.DB_DRIVER_KEY   , targetDbKey)).get.toString)
        .option("url"     , targetDbProperties.get(String.format(Common.DB_JDBC_URL     , targetDbKey)).get.toString)
        .option("user"    , targetDbProperties.get(String.format(Common.DB_USER_NAME_KEY, targetDbKey)).get.toString)
        .option("password", targetDbProperties.get(String.format(Common.DB_PASSWORD_KEY , targetDbKey)).get.toString)
        .option("dbtable" , table)
        .mode(SaveMode.Append)
        .save()
    }
    else if (Database.getDatabaseType(targetDbKey) == Database.GREENPLUM) {
      Common.log(getProcessId(), "Deleting the existing records from greenplum/postgre")
      val query = Common.buildDeleteQuery(idx, jsonParser)
      println(query)
      db.executeQuery(query)
    }
    if (Database.getDatabaseType(targetDbKey) != Database.MEMSQL) {
      sourceData.write
        .format("jdbc")
        .option("driver"  , targetDbProperties.get(String.format(Common.DB_DRIVER_KEY   , targetDbKey)).get.toString)
        .option("url"     , targetDbProperties.get(String.format(Common.DB_JDBC_URL     , targetDbKey)).get.toString)
        .option("user"    , targetDbProperties.get(String.format(Common.DB_USER_NAME_KEY, targetDbKey)).get.toString)
        .option("password", targetDbProperties.get(String.format(Common.DB_PASSWORD_KEY , targetDbKey)).get.toString)
        .option("dbtable" , jsonParser.getJsonProperty(String.format("%s-target-table", idx)).toString)
        .mode(SaveMode.Append)
        .save()
    }
    //Common.log(getProcessId(), "Clearing sourceData Cache")
    //sourceData.unpersist()
  }

  def executePreTasks(idx : Integer, db : Database): Unit = {
    var index : Integer = 0
    val loop  = new Breaks
    loop.breakable {
      while (true) {
        val preQuery = jsonParser.getJsonProperty(String.format("%s-pre-query-%s", idx, index))
        if (preQuery != null && !preQuery.toString.isEmpty) {
          Common.log(getProcessId(), String.format("Executing pre-executable query %s, %s", idx, index))
          println("Pre-query:")
          println(preQuery.toString)
          db.executeQuery(preQuery.toString)
        }
        else
          loop.break()
        index = index + 1
      }
    }
  }

  def executePostTasks(idx : Integer, db : Database): Unit = {
    var index : Integer = 0
    val loop  = new Breaks
    loop.breakable {
      while (true) {
        val postQuery = jsonParser.getJsonProperty(String.format("%s-post-query-%s", idx, index))
        if (postQuery != null && !postQuery.toString.isEmpty) {
          Common.log(getProcessId(), "Executing post-executable query "+index)
          println("Post-query:")
          println(postQuery.toString)
          db.executeQuery(postQuery.toString)
        }
        else
          loop.break()
        index = index + 1
      }
    }
  }

  def callDeltaCalc(): Unit = {
    if (idKey == "QryHVCurrentMRPLatest") {
      val post = new HttpPost(Common.cosaUrl)
      post.setEntity(new StringEntity("{}"))
      post.setHeader("Accept", "application/json");
      post.setHeader("Content-type", "application/json");
      val client = new DefaultHttpClient
      val response = client.execute(post)
      response.getAllHeaders.foreach(arg => println(arg))
      println(s"Status code after making the API call to ${Common.cosaUrl} is ${response.getStatusLine.getStatusCode}");
      Common.log(getProcessId(), "Called API to calculate bridge information and publish to cosa")
    }
  }

  def getProcessId() : String = {
    val id = jsonParser.getJsonProperty("process_id").toString
    if (!id.isEmpty) id else "1"
  }

  def buildUpdatedQuery(idx : Integer) : String = {
    var query   = jsonParser.getJsonProperty(String.format("%s-query", idx)).toString
    val diff    = hasDifferentColumnsInTarget(idx)
    val builder = new StringBuilder
    if (diff) {
      builder.append("SELECT ")
      var counter = 0
      for (field <- jsonParser.getJsonPropertiesStartsWith(String.format("%s-field-", idx))) {
        val loop = new Breaks
        loop.breakable {
          val f        = field._2.asInstanceOf[Map[String, Object]]
          val id       = f.get("id"         ).get.toString
          val target   = f.get("targetField").get.toString
          val autoIncr = Boolean.unbox(f.get("autoIncr").get)

          if (autoIncr) loop.break()
          builder.append(String.format("%s%s AS %s ", if(counter != 0) "," else "", id, target))
          counter = counter + 1
        }
      }
      builder.append(String.format("FROM (%s) src", query))
      query = builder.toString()
    }
    query
  }

  def hasDifferentColumnsInTarget(idx : Integer) : Boolean = {
    var result = false
    val loop   = new Breaks;
    loop.breakable {
      for (field <- jsonParser.getJsonPropertiesStartsWith(String.format("%s-field-", idx))) {
        if (result) loop.break()

        val f      = field._2.asInstanceOf[Map[String, Object]]
        val id     = f.get("id"         ).get.toString
        val target = f.get("targetField").get.toString
        if (!id.equalsIgnoreCase(target))
          result = true
      }
    }
    result
  }
}
