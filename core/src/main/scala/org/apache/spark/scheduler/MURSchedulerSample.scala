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
  private val taskRecordsWrite_cache = new ConcurrentHashMap[Long, ArrayBuffer[Long]]
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
    taskRecordsWrite_cache.put(taskId, new ArrayBuffer[Long])
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
    taskRecordsWrite_cache.remove(taskId)
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

  def updateWriteRecordsInCache(taskId: Long, readRecords: Long): Unit = {
    appendValue(taskId, taskRecordsWrite_cache, readRecords)
    currentTasksRecordsInputType.replace(taskId, 2)
  }

  def updateReadRecordsInCoCroup(taskId: Long, readRecords: Long): Unit = {
    appendValue(taskId, taskRecordsRead_cogroup, readRecords)
    currentTasksRecordsInputType.replace(taskId, 3)
  }

  def updateShuffleSampleResult(taskId: Long, sampleResult: Long): Unit = {
    appendValue(taskId, taskShuffleMemoryUsage, sampleResult)
  }

  def updateCacheSampleResult(taskId: Long, sampleResult: Long): Unit = {
    appendValue(taskId, taskCacheMemoryUsage, sampleResult)
    currentTasksMemoryUseType.replace(taskId, 1)
  }

  def appendValue(taskId: Long, mapWithBuffer: ConcurrentHashMap[Long, ArrayBuffer[Long]], value: Long): Unit = {
    if(mapWithBuffer.containsKey(taskId)) {
      val valueBuffer = mapWithBuffer.get(taskId)
      if(valueBuffer.size >= 1024){
        valueBuffer.remove(0, 512)
      }
      mapWithBuffer.replace(taskId, valueBuffer += value)
    }
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

    appendValue(taskId, taskBytesRead_input, bytesRead_input)
    appendValue(taskId, taskBytesRead_shuffle, bytesRead_shuffle)
    appendValue(taskId, taskRecordsRead_input, recordsRead_input)
    appendValue(taskId, taskRecordsRead_shuffle, recordsRead_shuffle)
    appendValue(taskId, taskBytesOutput, bytesOutput)
    appendValue(taskId, taskBytesShuffleWrite, bytesShuffleWrite)

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
  def getDeltaValue(valueBuffer: ArrayBuffer[Long]): Double = {
    if(valueBuffer == null)
      0.0
    else if(valueBuffer.length >1)
      valueBuffer.last-valueBuffer(valueBuffer.length-2)
    else
      0.0
  }

  def getBytesReadInput(taskId: Long) = getValue(taskBytesRead_input.get(taskId))
  def getBytesReadShuffle(taskId: Long) = getValue(taskBytesRead_shuffle.get(taskId))

  def getRecordsReadInput(taskId: Long) = getValue(taskRecordsRead_input.get(taskId))
  def getRecordsReadShuffle(taskId: Long) = getValue(taskRecordsRead_shuffle.get(taskId))
  def getRecordsReadCache(taskId: Long) = getValue(taskRecordsWrite_cache.get(taskId))
  def getRecordsReadCogroup(taskId: Long) = getValue(taskRecordsRead_cogroup.get(taskId))

  def getBytesOutput(taskId: Long) = getValue(taskBytesOutput.get(taskId))
  def getBytesShuffleWrite(taskId: Long) = getValue(taskBytesShuffleWrite.get(taskId))

  def getShuffleMemoryUsage(taskId: Long) = getValue(taskShuffleMemoryUsage.get(taskId))
  def getCacheMemoryUsage(taskId: Long) = getValue(taskCacheMemoryUsage.get(taskId))

  def getTasks(): Array[Long]= {
    val tasks = new Array[Long](taskTotalRecords.size())
    val keyIterator = taskTotalRecords.keySet().iterator()
    var index = 0
    while(keyIterator.hasNext){
      val taskId = keyIterator.next()
      tasks.update(index, taskId)
      index += 1
    }
    tasks
  }

  def getAllMemoryUsage(): Array[Long] = {
    val result = new Array[Long](currentTasksMemoryUseType.size())
    var index = 0
    val keyIterator = currentTasksMemoryUseType.keySet().iterator()
    while(keyIterator.hasNext){
      val taskId = keyIterator.next()
      currentTasksMemoryUseType.get(taskId) match{
        case 0 => result.update(index, getValue(taskShuffleMemoryUsage.get(taskId)))
        case 1 => result.update(index, getValue(taskCacheMemoryUsage.get(taskId)))
      }
      index += 1
    }
    result
  }

  def getMemoryUsage(taskId: Long): Long = {
    currentTasksMemoryUseType.get(taskId) match{
      case 0 => getValue(taskShuffleMemoryUsage.get(taskId))
      case 1 => getValue(taskCacheMemoryUsage.get(taskId))
      case _ => 0L
    }
  }

  def getBytesRead(taskId: Long): Long = {
    currentTasksBytesInputType.get(taskId) match{
      case 0 => getValue(taskBytesRead_input.get(taskId))
      case 1 => getValue(taskBytesRead_shuffle.get(taskId))
      case _ => 0
    }
  }

  def getMemoryUsageRate(taskId: Long): Double = {
    val deltaMemoryUsage = currentTasksMemoryUseType.get(taskId) match{
      case 0 => getDeltaValue(taskShuffleMemoryUsage.get(taskId))
      case 1 => getDeltaValue(taskCacheMemoryUsage.get(taskId))
      case _ => 0.0
    }
    val deltaInputRecords = currentTasksRecordsInputType.get(taskId) match{
      case 0 => getDeltaValue(taskRecordsRead_input.get(taskId))
      case 1 => getDeltaValue(taskRecordsRead_shuffle.get(taskId))
      case 2 => getDeltaValue(taskRecordsWrite_cache.get(taskId))
      case 3 => getDeltaValue(taskRecordsRead_cogroup.get(taskId))
      case _ => 0.0
    }
    val totalInputRecords = taskTotalRecords.get(taskId)
    val deltaInputBytes = currentTasksBytesInputType.get(taskId) match{
      case 0 => getDeltaValue(taskBytesRead_input.get(taskId))
      case 1 => getDeltaValue(taskBytesRead_shuffle.get(taskId))
      case _ => 0.0
    }
    if(deltaInputRecords != 0 && deltaInputBytes != 0)
      deltaMemoryUsage / deltaInputBytes
    else if(deltaInputBytes != 0 && deltaInputBytes == 0 && totalInputRecords != 0)
      deltaMemoryUsage / (deltaInputRecords / totalInputRecords * getBytesRead(taskId))
    else
      0.0
  }

  private val doingShuffleWrite = new ConcurrentHashMap[Long, Boolean]
  def addDoingShuffleWrite(taskId: Long): Unit ={
    doingShuffleWrite.put(taskId, true)
  }

  def getCompletePercent(taskId: Long): Double = {
    if(doingShuffleWrite.contains(taskId)){
      doingShuffleWrite.remove(taskId)
      return 1.0
    }
    val inputRecords = currentTasksRecordsInputType.get(taskId) match{
      case 0 => getValue(taskRecordsRead_input.get(taskId))
      case 1 => getValue(taskRecordsRead_shuffle.get(taskId))
      case 2 => getValue(taskRecordsWrite_cache.get(taskId))
      case 3 => getValue(taskRecordsRead_cogroup.get(taskId))
      case _ => 0
    }
    val totalInputRecords = taskTotalRecords.get(taskId)
    if(totalInputRecords != 0)
      inputRecords.toDouble / totalInputRecords.toDouble
    else
      // inputRecords.toDouble / 5400000
      0.0
  }

}
