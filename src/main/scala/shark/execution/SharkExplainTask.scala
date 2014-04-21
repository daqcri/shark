/*
 * Copyright (C) 2012 The Regents of The University California. 
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution

import java.io.PrintStream
import java.util.{HashSet => JHashSet, List => JList}

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{ExplainTask, Task}
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.{Context, DriverContext, QueryPlan}
import org.apache.hadoop.hive.ql.exec.{ExplainTask, Task}
import org.apache.hadoop.hive.ql.plan.ExplainWork
import org.apache.hadoop.util.StringUtils

import shark.LogHelper


class SharkExplainWork(
  resFile: String,
  rootTasks: JList[Task[_ <: java.io.Serializable]],
  astStringTree: String,
  inputs: JHashSet[ReadEntity],
  extended: Boolean) extends Serializable


/**
 * SharkExplainTask executes EXPLAIN for RDD operators.
 */
class SharkExplainTask extends Task[SharkExplainWork] with java.io.Serializable with LogHelper {

  val hiveExplainTask = new ExplainTask

  override def execute(driverContext: DriverContext): Int = {
    0
  }

  override def initialize(conf: HiveConf, queryPlan: QueryPlan, driverContext: DriverContext) {
    hiveExplainTask.initialize(conf, queryPlan, driverContext)
    super.initialize(conf, queryPlan, driverContext)
  }

  override def getType = hiveExplainTask.getType

  override def getName = hiveExplainTask.getName

  def localizeMRTmpFilesImpl(ctx: Context) {
    // explain task has nothing to localize
    // we don't expect to enter this code path at all
    throw new RuntimeException ("Unexpected call")
  }

}

