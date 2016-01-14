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

  // the second value of runningTasks save the total records of this task
  private val runningTasks = new ConcurrentHashMap[Long, Long]
  private val finishedTasks = new ArrayBuffer[Long]()
  private val mursStopTasks = new ArrayBuffer[Long]()

  // the processed bytes of each task
  private val taskBytesProcess = new ConcurrentHashMap[Long, Long]
  // the processed bytes increased how much
  private val taskBytesProcessIncrease = new ConcurrentHashMap[Long, Long]

  // the records which has been read by this task
  private val taskRecordsRead = new ConcurrentHashMap[Long, Long]

  // the memory usage of each task
  private val taskMemoryUsage = new ConcurrentHashMap[Long, Long]
  private val taskMemoryUsageRates = new ConcurrentHashMap[Long, ArrayBuffer[Double]]
  private val taskType = new ConcurrentHashMap[Long, String]

  def registerTask(taskId: Long): Unit = {
    runningTasks.put(taskId, 0L)
    runningTasksSampleFlag.put(taskId, false)
    taskBytesProcess.put(taskId, 0)
    taskBytesProcessIncrease.put(taskId, 0)
    taskRecordsRead.put(taskId, 0)
    taskMemoryUsage.put(taskId, 0)
  }

  def removeFinishedTask(taskId: Long): Unit = {
    runningTasks.remove(taskId)
    runningTasksSampleFlag.remove(taskId)
    taskBytesProcess.remove(taskId)
    taskBytesProcessIncrease.remove(taskId)
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
   * Before the task read it's input, we update the total records first
   */

  def updateTotalRecords(taskId: Long, totalRecords: Long): Unit = {
    runningTasks.replace(taskId, totalRecords)
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

    val bytesProcessIncrease = taskBytesProcessIncrease.get(taskId)
    val newMemoryUsageRate = taskMemoryUsageIncrease.toDouble / bytesProcessIncrease.toDouble
    if(taskMemoryUsageRates.contains(taskId)){
      taskMemoryUsageRates.get(taskId) += newMemoryUsageRate
    }else{
      val newResultBuffer = new ArrayBuffer[Double]
      taskMemoryUsageRates.put(taskId, newResultBuffer += newMemoryUsageRate)
    }
    logInfo(s"Task $taskId on executor $executorId has memory usage $sampleResult," +
      s" memory usage increase $taskMemoryUsageIncrease")
  }

  /**
   * Update the bytes read, memory usage and memory usage rate for one task, the metrics
   * and task memory manager is necessary. This method is used in executor
   * @param taskId
   * @param taskMetrics
   */
  def updateTaskInformation(taskId: Long, taskMetrics: TaskMetrics): Unit = {
    var bytesRead_input = 0L
    var bytesRead_shuffle = 0L
    var recordsRead_input = 0L
    var recordsRead_shuffle = 0L
    if(taskMetrics.inputMetrics.isDefined) {
      bytesRead_input = taskMetrics.inputMetrics.get.bytesRead
      recordsRead_input = taskMetrics.inputMetrics.get.recordsRead
    }
    if(taskMetrics.shuffleReadMetrics.isDefined) {
      bytesRead_shuffle = taskMetrics.shuffleReadMetrics.get.totalBytesRead
      recordsRead_shuffle = taskMetrics.shuffleReadMetrics.get.recordsRead
    }

    var memoryUsage_output = 0L
    var memoryUsage_shuffle = 0L
    if(taskMetrics.shuffleWriteMetrics.isDefined)
      memoryUsage_shuffle = taskMetrics.shuffleWriteMetrics.get.shuffleBytesWritten
    if(taskMetrics.outputMetrics.isDefined)
      memoryUsage_output = taskMetrics.outputMetrics.get.bytesWritten

    if(bytesRead_input == 0L && bytesRead_shuffle != 0L) {
      taskType.replace(taskId, "shuffle")
      val bytesProcess = bytesRead_shuffle * ( recordsRead_shuffle / runningTasks.get(taskId) )
      val bytesProcessIncrease = bytesProcess - taskBytesProcess.get(taskId)
      taskBytesProcess.replace(taskId, bytesProcess)
      taskBytesProcessIncrease.replace(taskId, bytesProcessIncrease)
    }
    else if (bytesRead_input != 0L && bytesRead_shuffle == 0L) {
      taskType.replace(taskId, "persist")
      val bytesProcess = bytesRead_input * ( recordsRead_input / runningTasks.get(taskId) )
      val bytesProcessIncrease = bytesProcess - taskBytesProcess.get(taskId)
      taskBytesProcess.replace(taskId, bytesProcess)
      taskBytesProcessIncrease.replace(taskId, bytesProcessIncrease)
    }
    else if (bytesRead_input != 0L && bytesRead_shuffle != 0L) {
      taskType.replace(taskId, "join")
      val bytesProcess =
        if(recordsRead_shuffle == 0L) {
          bytesRead_input * (recordsRead_input / runningTasks.get(taskId))
        } else {
          bytesRead_shuffle * (recordsRead_shuffle / runningTasks.get(taskId))
        }
      val bytesProcessIncrease = bytesProcess - taskBytesProcess.get(taskId)
      taskBytesProcess.replace(taskId, bytesProcess)
      taskBytesProcessIncrease.replace(taskId, bytesProcessIncrease)
    }

  }


}
