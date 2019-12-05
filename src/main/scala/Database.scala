package com.rajanainart

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

object Database extends Enumeration {
  type DbType = Value
  val MEMSQL, GREENPLUM, ORACLE, OTHER = Value

  def getDatabaseType(dbKey : String): DbType = {
    val dbProperties    = Common.dbConfig.getJsonPropertiesStartsWith(dbKey)
    val driverClassKey  = String.format(Common.DB_DRIVER_KEY, dbKey)
    val driverClassName = String.valueOf(dbProperties.get(driverClassKey))
    var result : DbType = OTHER

    if (driverClassName.indexOf("mysql") >= 0)
      result = MEMSQL
    else if (driverClassName.indexOf("postgre") >= 0)
      result = GREENPLUM
    else if (driverClassName.indexOf("oracle") >= 0)
      result = ORACLE
    result
  }
}

class Database (val dbKey : String) extends AutoCloseable {
  val dbProperties = Common.dbConfig.getJsonPropertiesStartsWith(dbKey)

  val jdbcOptions : JDBCOptions = new JDBCOptions(Map(
    JDBCOptions.JDBC_URL -> String.valueOf(dbProperties.get(String.format(Common.DB_JDBC_URL, dbKey)).get),
    "user"     -> String.valueOf(dbProperties.get(String.format(Common.DB_USER_NAME_KEY, dbKey)).get),
    "password" -> String.valueOf(dbProperties.get(String.format(Common.DB_PASSWORD_KEY , dbKey)).get),
    "driver"   -> String.valueOf(dbProperties.get(String.format(Common.DB_DRIVER_KEY   , dbKey)).get),
    "dbtable"  -> "(SELECT 1 AS col1) src1"
  ))
  val connection = JdbcUtils.createConnectionFactory(jdbcOptions).apply()

  def executeQuery(query : String) {
    val statement = connection.prepareStatement(query)
    statement.execute()
    //connection.commit()
  }

  def close(): Unit = {
    if (connection != null && !connection.isClosed)
      connection.close()
  }
}
