package com.rajanainart

import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.writer._
import java.util.Optional

import org.apache.spark.sql.SaveMode
import java.sql.DriverManager

import org.apache.spark.sql.catalyst.InternalRow

class DefaultSource extends DataSourceV2 with WriteSupport {

  def createWriter(jobId  : String,
                   schema : StructType,
                   mode   : SaveMode,
                   options: DataSourceOptions) : Optional[DataSourceWriter] = {
    Optional.of(new MemSqlDataSourceWriter(schema))
  }
}

class MemSqlDataSourceWriter (schema : StructType) extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new MemSqlDataWriterFactory(schema)
  }

  override def commit(messages: Array[WriterCommitMessage]) = {
  }

  override def abort (messages: Array[WriterCommitMessage]) = {
    println("abort is called in  data source writer")
  }
}

class MemSqlDataWriterFactory (schema : StructType) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId : Int, taskId : Long, epochId : Long): DataWriter[InternalRow] = {
    new MemSqlDataWriter(schema)
  }
}

class MemSqlDataWriter (schema : StructType) extends DataWriter[InternalRow] {
  val memSqlDbKey   = String.valueOf(Common.appConfig.getJsonProperty("memsql-db-key"))
  val memSqlDbProps = Common.dbConfig.getJsonPropertiesStartsWith(memSqlDbKey)

  val driver   = memSqlDbProps.get(String.format(Common.DB_DRIVER_KEY   , memSqlDbKey)).get.toString
  val url      = memSqlDbProps.get(String.format(Common.DB_JDBC_URL     , memSqlDbKey)).get.toString
  val user     = memSqlDbProps.get(String.format(Common.DB_USER_NAME_KEY, memSqlDbKey)).get.toString
  val password = memSqlDbProps.get(String.format(Common.DB_PASSWORD_KEY , memSqlDbKey)).get.toString

  val connection = DriverManager.getConnection(url, user, password)
  val statement  = Common.getMemSqlBaseUpdateQuery()
  val preparedStatement = connection.prepareStatement(statement)
  connection.setAutoCommit(false)

  var currentRow = 0

  def write(record: InternalRow) = {
    currentRow = currentRow + 1
    var index      = 0
    var nullFields = 0
    for (field <- schema.fields) {
      //if (index == 0)
      //  println("Field1:"+record.get(index, field.dataType))
      val value = record.get(index, field.dataType)
      if (value == null) {
        nullFields = nullFields + 1
        preparedStatement.setObject(index+1, value)
      }
      else
        preparedStatement.setObject(index+1, value.toString)

      index = index + 1
    }
    if (nullFields != schema.fields.size)
      preparedStatement.executeUpdate()

    if (currentRow % 100 == 0)
      println(printf("Completed processing %d records", currentRow))
  }

  def commit(): WriterCommitMessage = {
    connection.commit()
    WriteSucceeded
  }

  def abort () = {
    println("abort is called in data writer")
  }

  object WriteSucceeded extends WriterCommitMessage
}
