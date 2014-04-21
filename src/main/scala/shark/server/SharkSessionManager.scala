package shark.server

import java.util.concurrent.Executors;


import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.cli.operation.OperationManager;


import shark.Utils

class SharkSessionManager extends SessionManager {
  override def init(hiveConf: HiveConf): Unit = synchronized {
    Utils.setSuperField("hiveConf", hiveConf, this)

    val backgroundPoolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS);
    Utils.setSuperField("backgroundOperationPool", Executors.newFixedThreadPool(backgroundPoolSize), this)

    val sharkOpManager = new SharkOperationManager
    Utils.setSuperField("operationManager", sharkOpManager, this)
    addService(sharkOpManager)

    sharkInit(hiveConf)
  }
}
