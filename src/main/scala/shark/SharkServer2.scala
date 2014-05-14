package shark

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}
import org.apache.hive.service.CompositeService
import org.apache.spark.SparkEnv
import shark.server.SharkCLIService

import scala.collection.JavaConversions._

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object SharkServer2 extends LogHelper {
  var LOG = LogFactory.getLog(classOf[SharkServer2])

  val sparkContext = new SparkContext("local", "")
  val hiveContext = new HiveContext(sparkContext)

  server.SharkServer.hiveContext = hiveContext

  def main(args: Array[String]) {
    try {
      LogUtils.initHiveLog4j()
    } catch {
      case e: LogInitializationException => {
        LOG.warn(e.getMessage)
      }
    }
    val optproc = new ServerOptionsProcessor("sharkserver2") //TODO: include load RDDs

    if (!optproc.process(args)) {
      LOG.fatal("Error starting SharkServer2 with given arguments")
      System.exit(-1)
    }

    Runtime.getRuntime.addShutdownHook(
      new Thread() {
        override def run() {
          sparkContext.stop()
        }
      }
    )
  }

  try {
    val hiveConf = new HiveConf
    val server = new SharkServer2
    server.init(hiveConf)
    server.start()
    logInfo("SharkServer2 started")
  } catch {
    case t: Exception => {
      LOG.fatal("Error starting SharkServer2", t)
      System.exit(-1)
    }
  }
}

class SharkServer2 extends HiveServer2 {
  override def init(hiveConf: HiveConf): Unit = synchronized {
    val sharkCLIService = new SharkCLIService
    Utils.setSuperField("cliService", sharkCLIService, this)
    addService(sharkCLIService)
    val sthriftCLIService = new ThriftBinaryCLIService(sharkCLIService)
    Utils.setSuperField("thriftCLIService", sthriftCLIService, this)
    addService(sthriftCLIService)
    sharkInit(hiveConf)
  }
}


