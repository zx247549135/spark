package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.Logging
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager

import scala.collection.mutable.ArrayBuffer

/**
 * Created by zx on 16-1-8.
 *
 * This Scheduler is based on Memory-Usage-Rate, and running on executor.
 */
class MURScheduler(
     executorId: String) extends Serializable with Logging {

  private val runningTasks = new ConcurrentHashMap[Long, TaskMemoryManager]
  private val mursStopTasks = new ArrayBuffer[Long]()
  private val taskBytesRead = new ConcurrentHashMap[Long, Long]
  private val taskMemoryUsage = new ConcurrentHashMap[Long, Long]
  private val taskMemoryUsageRates = new ConcurrentHashMap[Long, ArrayBuffer[Double]]

  def registerTask(taskId: Long, taskMemoryManager: TaskMemoryManager): Unit = {
    runningTasks.put(taskId, taskMemoryManager)
    taskBytesRead.put(0,0)
    taskMemoryUsage.put(0,0)
  }

  def removeFinishedTask(taskId: Long): Unit = {
    runningTasks.remove(taskId)
    taskBytesRead.remove(taskId)
    taskMemoryUsage.remove(taskId)
    taskMemoryUsageRates.remove(taskId)
  }

  def registerStopTask(taskId: Long): Unit = {
    mursStopTasks += taskId
  }

  /**
   * Update the bytes read, memory usage and memory usage rate for one task, the metrics
   * and task memory manager is necessary. This method is used in executor
   * @param taskId
   * @param taskMetrics
   */
  def updateTaskInformation(taskId: Long,
                            taskMetrics: TaskMetrics): Unit = {
    val bytesRead =
      if(taskMetrics.inputMetrics.isDefined)
        taskMetrics.inputMetrics.get.bytesRead
      else
        taskMetrics.shuffleReadMetrics.get.totalBytesRead
    val taskMemoryManager = runningTasks.get(taskId)
    val memoryUsage = taskMemoryManager.getMemoryConsumptionForThisTask
    val newMemoryUsageRate = (memoryUsage - taskMemoryUsage.get(taskId)).toDouble /
      (bytesRead - taskBytesRead.get(taskId)).toDouble
    val memoryUsageRateBuffer = taskMemoryUsageRates.get(taskId)

    taskMemoryUsageRates.replace(taskId, memoryUsageRateBuffer += newMemoryUsageRate)
    taskBytesRead.replace(taskId, bytesRead)
    taskMemoryUsage.replace(taskId, memoryUsage)

    logInfo(s"Task $taskId on executor $executorId has memory usage rate $newMemoryUsageRate")
  }


}
