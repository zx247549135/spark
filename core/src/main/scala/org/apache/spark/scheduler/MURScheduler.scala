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
  private val taskBytesRead_input = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskBytesRead_shuffle = new ConcurrentHashMap[Long, ArrayBuffer[Long]]

  // the records which has been read by this task
  private val taskRecordsRead_input = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskRecordsRead_shuffle = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskRecordsRead_cache = new ConcurrentHashMap[Long, ArrayBuffer[Long]]

  private val taskBytesShuffleWrite = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskBytesOutput = new ConcurrentHashMap[Long, ArrayBuffer[Long]]

  // the memory usage of each task
  private val taskMemoryUsage = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskCacheMemoryUsage = new ConcurrentHashMap[Long, ArrayBuffer[Long]]

  def showMessage(taskId: Long): Unit = {

    if( ! runningTasks.containsKey(taskId) )
      return

    val totalRecords = runningTasks.get(taskId)
    def getValue(valueBuffer: ArrayBuffer[Long]): Long = {
      if (valueBuffer.length != 0 )
        valueBuffer.last
      else
        0L
    }

    val bytesRead_input = getValue(taskBytesRead_input.get(taskId))
    val bytesRead_shuffle = getValue(taskBytesRead_shuffle.get(taskId))
    val recordsRead_input = getValue(taskRecordsRead_input.get(taskId))
    val recordsRead_shuffle = getValue(taskRecordsRead_shuffle.get(taskId))
    val recordsRead_cache = getValue(taskRecordsRead_cache.get(taskId))
    val bytesOutput = getValue(taskBytesOutput.get(taskId))
    val bytesShuffleWrite = getValue(taskBytesShuffleWrite.get(taskId))
    val memoryUsage = getValue(taskMemoryUsage.get(taskId))
    val cacheMemoryUsage = getValue(taskCacheMemoryUsage.get(taskId))
    if(memoryUsage != 0 && taskId % 4 == 0)
      logInfo(s"Task $taskId has bytes read $bytesRead_input/$bytesRead_shuffle, " +
        s"records $totalRecords, read records $recordsRead_input/$recordsRead_shuffle/$recordsRead_cache, " +
        s"bytes output $bytesOutput, shuffle write $bytesShuffleWrite, " +
        s"memory usage $memoryUsage/$cacheMemoryUsage.")
  }

  def registerTask(taskId: Long): Unit = {
    runningTasks.put(taskId, 0L)
    runningTasksSampleFlag.put(taskId, false)
    taskBytesRead_input.put(taskId, new ArrayBuffer[Long])
    taskBytesRead_shuffle.put(taskId, new ArrayBuffer[Long])
    taskRecordsRead_input.put(taskId, new ArrayBuffer[Long])
    taskRecordsRead_shuffle.put(taskId, new ArrayBuffer[Long])
    taskRecordsRead_cache.put(taskId, new ArrayBuffer[Long])
    taskBytesOutput.put(taskId, new ArrayBuffer[Long])
    taskBytesShuffleWrite.put(taskId, new ArrayBuffer[Long])
    taskMemoryUsage.put(taskId, new ArrayBuffer[Long])
    taskCacheMemoryUsage.put(taskId, new ArrayBuffer[Long])
  }

  def removeFinishedTask(taskId: Long): Unit = {
    runningTasks.remove(taskId)
    runningTasksSampleFlag.remove(taskId)
    taskBytesRead_input.remove(taskId)
    taskBytesRead_shuffle.remove(taskId)
    taskRecordsRead_input.remove(taskId)
    taskRecordsRead_shuffle.remove(taskId)
    taskRecordsRead_cache.remove(taskId)
    taskBytesOutput.remove(taskId)
    taskBytesShuffleWrite.remove(taskId)
    taskMemoryUsage.remove(taskId)
    taskCacheMemoryUsage.remove(taskId)
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

  // this method will only be used in cache operation
  def updateReadRecordsInCache(taskId: Long, readRecords: Long): Unit = {
    val recordsBuffer = taskRecordsRead_cache.get(taskId)
    taskRecordsRead_cache.replace(taskId, recordsBuffer += readRecords)
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

    val taskMemoryUsageBuffer = taskMemoryUsage.get(taskId)
    taskMemoryUsage.replace(taskId, taskMemoryUsageBuffer += sampleResult)
  }

  def updateCacheSampleResult(taskId: Long, sampleResult: Long): Unit = {
    updateSingleTaskSampleFlag(taskId)

    val taskCacheMemoryUsageBuffer = taskCacheMemoryUsage.get(taskId)
    taskCacheMemoryUsage.replace(taskId, taskCacheMemoryUsageBuffer += sampleResult)
  }

  /**
   * Update the bytes read, memory usage and memory usage rate for one task, the metrics
   * and task memory manager is necessary. This method is used in executor
   * @param taskId
   * @param taskMetrics the mertrics contain the input record and shuffle write to file
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

    var bytesOutput = 0L
    var bytesShuffleWrite = 0L

    if(taskMetrics.outputMetrics.isDefined)
      bytesOutput = taskMetrics.outputMetrics.get.bytesWritten
    if(taskMetrics.shuffleWriteMetrics.isDefined)
      bytesShuffleWrite = taskMetrics.shuffleWriteMetrics.get.shuffleBytesWritten

    def appendValue(mapWithBuffer: ConcurrentHashMap[Long, ArrayBuffer[Long]], value: Long): Unit = {
      val valueBuffer = mapWithBuffer.get(taskId)
      mapWithBuffer.replace(taskId, valueBuffer += value)
    }

    appendValue(taskBytesRead_input, bytesRead_input)
    appendValue(taskBytesRead_shuffle, bytesRead_shuffle)
    appendValue(taskRecordsRead_input, recordsRead_input)
    appendValue(taskRecordsRead_shuffle, recordsRead_shuffle)
    appendValue(taskBytesOutput, bytesOutput)
    appendValue(taskBytesShuffleWrite, bytesShuffleWrite)

  }


}
