package org.apache.spark.scheduler

import org.apache.spark.Logging

/**
 * Created by zx on 16-5-20.
 */
class MURSchedulerMemory(totalMemory: Long,
                         yellowLine: Double,
                         redLine: Double,
                         mursSample: MURSchedulerSample) extends Serializable with Logging{

  val youngGeneration = (totalMemory * 0.33).toLong
  val oldGeneration = (totalMemory * 0.66).toLong

  val yellowMemory = (oldGeneration * yellowLine).toLong
  val redMemory = (oldGeneration * redLine).toLong

  def buildMemoryUsage(runningTasks: Array[Long]): Unit ={

  }

}
