package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.Logging
import org.apache.spark.executor.TaskMetrics

import scala.collection.mutable.ArrayBuffer

/**
 * Created by zx on 16-1-20.
 */
class MURSchedulerSample extends Serializable with Logging{

  // total bytes read by this task through input or shuffle
  private val taskBytesRead_input = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskBytesRead_shuffle = new ConcurrentHashMap[Long, ArrayBuffer[Long]]

  // the records which should be read or have been read by this task
  // the read records contains four type: from cache, from input, from shuffle and from cogroup
  private val taskTotalRecords = new ConcurrentHashMap[Long, Long]
  private val taskRecordsRead_input = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskRecordsRead_shuffle = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskRecordsRead_cache = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskRecordsRead_cogroup = new ConcurrentHashMap[Long, ArrayBuffer[Long]]

  // total bytes write by this task, shuffle write and output are two different ways
  private val taskBytesShuffleWrite = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskBytesOutput = new ConcurrentHashMap[Long, ArrayBuffer[Long]]

  // the memory usage of each task: shuffle used or cache used
  private val taskShuffleMemoryUsage = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
  private val taskCacheMemoryUsage = new ConcurrentHashMap[Long, ArrayBuffer[Long]]


  private val currentTasksBytesInputType = new ConcurrentHashMap[Long, Int]
  // The input type of this task is from input disk/ cache block/ shuffle read/ cogroup
  private val currentTasksRecordsInputType = new ConcurrentHashMap[Long, Int]
  // The memory usage type can be shuffle and cache
  private val currentTasksMemoryUseType = new ConcurrentHashMap[Long, Int]

  def registerTask(taskId: Long): Unit = {
    taskBytesRead_input.put(taskId, new ArrayBuffer[Long])
    taskBytesRead_shuffle.put(taskId, new ArrayBuffer[Long])

    taskTotalRecords.put(taskId, 0L)
    taskRecordsRead_input.put(taskId, new ArrayBuffer[Long])
    taskRecordsRead_shuffle.put(taskId, new ArrayBuffer[Long])
    taskRecordsRead_cache.put(taskId, new ArrayBuffer[Long])
    taskRecordsRead_cogroup.put(taskId, new ArrayBuffer[Long])

    taskBytesOutput.put(taskId, new ArrayBuffer[Long])
    taskBytesShuffleWrite.put(taskId, new ArrayBuffer[Long])

    taskShuffleMemoryUsage.put(taskId, new ArrayBuffer[Long])
    taskCacheMemoryUsage.put(taskId, new ArrayBuffer[Long])

    currentTasksBytesInputType.put(taskId, 0)
    currentTasksRecordsInputType.put(taskId, 0)
    currentTasksMemoryUseType.put(taskId, 0)
  }

  def removeFinishedTask(taskId: Long): Unit ={
    taskBytesRead_input.remove(taskId)
    taskBytesRead_shuffle.remove(taskId)

    taskTotalRecords.remove(taskId)
    taskRecordsRead_input.remove(taskId)
    taskRecordsRead_shuffle.remove(taskId)
    taskRecordsRead_cache.remove(taskId)
    taskRecordsRead_cogroup.remove(taskId)

    taskBytesOutput.remove(taskId)
    taskBytesShuffleWrite.remove(taskId)

    taskShuffleMemoryUsage.remove(taskId)
    taskCacheMemoryUsage.remove(taskId)

    currentTasksBytesInputType.remove(taskId)
    currentTasksRecordsInputType.remove(taskId)
    currentTasksMemoryUseType.remove(taskId)
  }

  def updateTotalRecords(taskId: Long, totalRecords: Long): Unit = {
    taskTotalRecords.replace(taskId, totalRecords)
  }

  def updateReadRecordsInCache(taskId: Long, readRecords: Long): Unit = {
    val recordsBuffer = taskRecordsRead_cache.get(taskId)
    taskRecordsRead_cache.replace(taskId, recordsBuffer += readRecords)
    currentTasksRecordsInputType.replace(taskId, 2)
  }

  def updateReadRecordsInCoCroup(taskId: Long, readRecords: Long): Unit = {
    val recordsBuffer = taskRecordsRead_cogroup.get(taskId)
    taskRecordsRead_cogroup.replace(taskId, recordsBuffer += readRecords)
    currentTasksRecordsInputType.replace(taskId, 3)
  }

  def updateShuffleSampleResult(taskId: Long, sampleResult: Long): Unit = {
    val taskMemoryUsageBuffer = taskShuffleMemoryUsage.get(taskId)
    taskShuffleMemoryUsage.replace(taskId, taskMemoryUsageBuffer += sampleResult)
  }

  def updateCacheSampleResult(taskId: Long, sampleResult: Long): Unit = {
    val taskCacheMemoryUsageBuffer = taskCacheMemoryUsage.get(taskId)
    taskCacheMemoryUsage.replace(taskId, taskCacheMemoryUsageBuffer += sampleResult)
    currentTasksMemoryUseType.replace(taskId, 1)
  }

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
      currentTasksBytesInputType.replace(taskId, 1)
      recordsRead_shuffle = taskMetrics.shuffleReadMetrics.get.recordsRead
      currentTasksRecordsInputType.replace(taskId, 1)
    }

    var bytesOutput = 0L
    var bytesShuffleWrite = 0L

    if(taskMetrics.outputMetrics.isDefined)
      bytesOutput = taskMetrics.outputMetrics.get.bytesWritten
    if(taskMetrics.shuffleWriteMetrics.isDefined)
      bytesShuffleWrite = taskMetrics.shuffleWriteMetrics.get.shuffleBytesWritten

    def appendValue(mapWithBuffer: ConcurrentHashMap[Long, ArrayBuffer[Long]], value: Long): Unit = {
      if(mapWithBuffer.containsKey(taskId)) {
        val valueBuffer = mapWithBuffer.get(taskId)
        mapWithBuffer.replace(taskId, valueBuffer += value)
      }
    }

    appendValue(taskBytesRead_input, bytesRead_input)
    appendValue(taskBytesRead_shuffle, bytesRead_shuffle)
    appendValue(taskRecordsRead_input, recordsRead_input)
    appendValue(taskRecordsRead_shuffle, recordsRead_shuffle)
    appendValue(taskBytesOutput, bytesOutput)
    appendValue(taskBytesShuffleWrite, bytesShuffleWrite)

  }

  def getTotalRecords(taskId: Long) = taskTotalRecords.get(taskId)

  def getValue(valueBuffer: ArrayBuffer[Long]): Long = {
    if (valueBuffer == null)
      0L
    else if (valueBuffer.length != 0 )
      valueBuffer.last
    else
      0L
  }
  //return the delta value of an ArrayBuffer's last two value
  def getDeltaValue(valueBuffer: ArrayBuffer[Long]): Long={
    if(valueBuffer == null)
      0L
    else if(valueBuffer.length != 0)
      valueBuffer.last-valueBuffer(valueBuffer.length-2)
    else
      0L


  }

  def getBytesReadInput(taskId: Long) = getValue(taskBytesRead_input.get(taskId))
  def getBytesReadShuffle(taskId: Long) = getValue(taskBytesRead_shuffle.get(taskId))

  def getRecordsReadInput(taskId: Long) = getValue(taskRecordsRead_input.get(taskId))
  def getRecordsReadShuffle(taskId: Long) = getValue(taskRecordsRead_shuffle.get(taskId))
  def getRecordsReadCache(taskId: Long) = getValue(taskRecordsRead_cache.get(taskId))
  def getRecordsReadCogroup(taskId: Long) = getValue(taskRecordsRead_cogroup.get(taskId))

  def getBytesOutput(taskId: Long) = getValue(taskBytesOutput.get(taskId))
  def getBytesShuffleWrite(taskId: Long) = getValue(taskBytesShuffleWrite.get(taskId))

  def getShuffleMemoryUsage(taskId: Long) = getValue(taskShuffleMemoryUsage.get(taskId))
  def getCacheMemoryUsage(taskId: Long) = getValue(taskCacheMemoryUsage.get(taskId))

  def getAllBytesRead(): Array[Long] = {
    val result = new Array[Long](currentTasksBytesInputType.size())
    var index = 0
    val keyIterator = currentTasksBytesInputType.keySet().iterator()
    while(keyIterator.hasNext){
      val taskId = keyIterator.next()
      currentTasksBytesInputType.get(taskId) match{
        case 0 => result.update(index, getValue(taskBytesRead_input.get(taskId)))
        case 1 => result.update(index, getValue(taskBytesRead_shuffle.get(taskId)))
      }
      index += 1
    }
    result
  }

  def getAllRecordsReadDeltaValue(): Array[Long] = {
    val result = new Array[Long](currentTasksRecordsInputType.size())
    var index = 0
    val keyIterator = currentTasksRecordsInputType.keySet().iterator()
    while(keyIterator.hasNext){
      val taskId = keyIterator.next()
      currentTasksRecordsInputType.get(taskId) match{
        case 0 => result.update(index, getDeltaValue(taskRecordsRead_input.get(taskId)))
        case 1 => result.update(index, getDeltaValue(taskRecordsRead_shuffle.get(taskId)))
        case 2 => result.update(index, getDeltaValue(taskRecordsRead_cache.get(taskId)))
        case 3 => result.update(index, getDeltaValue(taskRecordsRead_cogroup.get(taskId)))
      }
      index += 1
    }
    result
  }

  def getAllTotalRecordsRead(): (Array[Long], Array[Long]) = {
    val result = new Array[Long](taskTotalRecords.size())
    val tasks = new Array[Long](taskTotalRecords.size())
    var index = 0
    val keyIterator = taskTotalRecords.keySet().iterator()
    while(keyIterator.hasNext){
      val taskId = keyIterator.next()
      result.update(index, taskTotalRecords.get(taskId))
      tasks.update(index, taskId)
      index += 1
    }
    (tasks,result)
  }

  def getAllMemoryUsageDeltaValue(): Array[Long] = {
    val result = new Array[Long](currentTasksMemoryUseType.size())
    var index = 0
    val keyIterator = currentTasksMemoryUseType.keySet().iterator()
    while(keyIterator.hasNext){
      val taskId = keyIterator.next()
      currentTasksMemoryUseType.get(taskId) match{
        case 0 => result.update(index, getDeltaValue(taskShuffleMemoryUsage.get(taskId)))
        case 1 => result.update(index, getDeltaValue(taskCacheMemoryUsage.get(taskId)))
      }
      index += 1
    }
    result
  }

}
