package shark
package server

import java.util.{Map => JMap}
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession

import org.apache.hive.service.cli.{HiveSQLException, OperationState, TableSchema}

import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.operation.Operation;


import org.apache.hive.service.cli.FetchOrientation
import org.apache.hive.service.cli.RowSet
import org.apache.hive.service.cli.TableSchema
import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.hive.HiveContext

import org.apache.hive.service.cli.Row
import org.apache.hive.service.cli.ColumnValue;


import scala.collection.JavaConversions._

object SharkServer {
  var hiveContext: HiveContext = null
}

class SharkOperationManager extends OperationManager {
  val handleToOperation = Utils.getSuperField("handleToOperation", this).asInstanceOf[java.util.Map[OperationHandle, Operation]]

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {

    val operation = new ExecuteStatementOperation(parentSession, statement, confOverlay) {
      var result: SchemaRDD = _
      var done: Boolean = false

      def close(): Unit = println("CLOSING")
      def getNextRowSet(order: FetchOrientation, maxRows: Long): RowSet = {

        if (done) {
          new RowSet()
        } else {
          done = true
          new RowSet(result.collect().map { cRow => {
            val row = new Row()
            cRow.foreach {
              case s: String => row.addString(s)
              case i: Int => row.addColumnValue(ColumnValue.intValue(i))
            }
            row
          }}.toList, 0)
        }
      }

      def getResultSetSchema(): TableSchema = {
        println("TABLESCHEMA")

        val schema = result.queryExecution.analyzed.output.map { attr =>
          new FieldSchema(attr.name, org.apache.spark.sql.hive.HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
        }

        println(schema)

        new TableSchema(schema)
      }
      def run(): Unit = {
        setState(OperationState.RUNNING)
        println(s"RUNNING $statement")
        try {
          println("TRY")
          println(SharkServer.hiveContext)
          result = SharkServer.hiveContext.hql(statement)
          println("DONEISH")
          setHasResultSet(true)
        } catch {
          case e: Throwable =>
            println(e)
            e.getStackTrace.foreach(println)
        }
        println("DONE")
        setState(OperationState.FINISHED)
      }
    }

   handleToOperation.put(operation.getHandle, operation)
   operation
  }
}
