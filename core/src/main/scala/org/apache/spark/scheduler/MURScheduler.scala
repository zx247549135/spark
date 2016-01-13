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

  private val runningTasksSampleFlag = new ConcurrentHashMap[Long, Boolean]

  private val runningTasks = new ConcurrentHashMap[Long, TaskMemoryManager]
  private val finishedTasks = new ArrayBuffer[Long]()
  private val mursStopTasks = new ArrayBuffer[Long]()
  private val taskBytesRead = new ConcurrentHashMap[Long, Long]
  private val taskMemoryUsage = new ConcurrentHashMap[Long, Long]
  private val taskMemoryUsageRates = new ConcurrentHashMap[Long, ArrayBuffer[Double]]

  def registerTask(taskId: Long, taskMemoryManager: TaskMemoryManager): Unit = {
    runningTasks.put(taskId, taskMemoryManager)
    runningTasksSampleFlag.put(taskId, false)
    taskBytesRead.put(0,0)
    taskMemoryUsage.put(0,0)
  }

  def removeFinishedTask(taskId: Long): Unit = {
    runningTasks.remove(taskId)
    runningTasksSampleFlag.remove(taskId)
    taskBytesRead.remove(taskId)
    taskMemoryUsage.remove(taskId)
    taskMemoryUsageRates.remove(taskId)
  }

  def registerFinishedTask(taskId: Long): Unit = {
    finishedTasks += taskId
  }

  def registerStopTask(taskId: Long): Unit = {
    mursStopTasks += taskId
  }

  /**
   * Follow functions are used to control the sample in tasks
   */

  def getSampleFlag(taskId: Long): Boolean = {
    runningTasksSampleFlag.get(taskId)
  }

  /**
   * This method is called by MURS thread in interval times.
   */

  def updateAllSampleFlag(): Unit = {
    val keyIter = runningTasksSampleFlag.keySet.iterator()
    while(keyIter.hasNext)
      runningTasksSampleFlag.replace(keyIter.next(), true)
  }

  def updateSingleTaskSampleFlag(taskId: Long): Unit = {
    runningTasksSampleFlag.replace(taskId, false)
  }

  /**
   * This method is called by task when task is writing records. After the task call
   * this method, the sample flag of it should be false
   * @param taskId which task will send the sample result
   * @param sampleResult the new sample result
   */

  def updateSampleResult(taskId: Long, sampleResult: Long): Unit = {
    updateSingleTaskSampleFlag(taskId)
    var taskMemoryUsageIncrease = 0L
    if(taskMemoryUsage.contains(taskId)){
      taskMemoryUsageIncrease = sampleResult - taskMemoryUsage.get(taskId)
      taskMemoryUsage.replace(taskId, sampleResult)
    }else {
      taskMemoryUsageIncrease = sampleResult
      taskMemoryUsage.put(taskId, sampleResult)
    }
    val bytesRead = taskBytesRead.get(taskId)
    val newMemoryUsageRate = taskMemoryUsageIncrease.toDouble / bytesRead.toDouble
    if(taskMemoryUsageRates.contains(taskId)){
      taskMemoryUsageRates.get(taskId) += newMemoryUsageRate
    }else{
      val newResultBuffer = new ArrayBuffer[Double]
      taskMemoryUsageRates.put(taskId, newResultBuffer += newMemoryUsageRate)
    }
    logInfo(s"Task $taskId on executor $executorId has bytes read $bytesRead, memory usage increase $taskMemoryUsageIncrease")
  }

  /**
   * Update the bytes read, memory usage and memory usage rate for one task, the metrics
   * and task memory manager is necessary. This method is used in executor
   * @param taskId
   * @param taskMetrics
   */
  def updateTaskInformation(taskId: Long,
                            taskMetrics: TaskMetrics): Unit = {
    var bytesRead = 0L
    if(taskMetrics.inputMetrics.isDefined)
      bytesRead = taskMetrics.inputMetrics.get.bytesRead
    else if(taskMetrics.shuffleReadMetrics.isDefined && bytesRead == 0L)
      bytesRead = taskMetrics.shuffleReadMetrics.get.totalBytesRead
//    val taskMemoryManager = runningTasks.get(taskId)
//    val memoryUsage = taskMemoryManager.getMemoryConsumptionForThisTask
    var memoryUsage = 0L
    if(taskMetrics.shuffleWriteMetrics.isDefined)
      memoryUsage = taskMetrics.shuffleWriteMetrics.get.shuffleBytesWritten
    else if(taskMetrics.outputMetrics.isDefined && memoryUsage == 0L)
      memoryUsage = taskMetrics.outputMetrics.get.bytesWritten

    var taskMemoryUsageIncrease = 0L
    if(taskMemoryUsage.contains(taskId)){
      taskMemoryUsageIncrease = memoryUsage - taskMemoryUsage.get(taskId)
      taskMemoryUsage.replace(taskId, memoryUsage)
    }else {
      taskMemoryUsageIncrease = memoryUsage
      taskMemoryUsage.put(taskId, memoryUsage)
    }
    var bytesReadIncrease = 0L
    if(taskBytesRead.containsKey(taskId)){
      bytesReadIncrease = bytesRead - taskBytesRead.get(taskId)
      taskBytesRead.replace(taskId, bytesRead)
    }else{
      bytesReadIncrease = bytesRead
      taskBytesRead.put(taskId, bytesRead)
    }
    val newMemoryUsageRate = taskMemoryUsageIncrease.toDouble / bytesReadIncrease.toDouble

    // if the task memory usage rate is first build, it can't be get by the method get().
    if (taskMemoryUsageRates.containsKey(taskId)) {
      val memoryUsageRateBuffer = taskMemoryUsageRates.get(taskId)
      taskMemoryUsageRates.replace(taskId, memoryUsageRateBuffer += newMemoryUsageRate)
    } else {
      val memoryUsageRateBuffer = new ArrayBuffer[Double]
      memoryUsageRateBuffer += newMemoryUsageRate
      taskMemoryUsageRates.put(taskId, memoryUsageRateBuffer)
    }

    logInfo(s"Task $taskId on executor $executorId has bytes read $bytesRead, memory usage $memoryUsage")
    logInfo(s"Task $taskId on executor $executorId increase bytes read $bytesReadIncrease, memory usage $taskMemoryUsageIncrease")
  }


}
