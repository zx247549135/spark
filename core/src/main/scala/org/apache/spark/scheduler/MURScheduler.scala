package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{SparkEnv, SparkConf, Logging}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager

import scala.collection.mutable.ArrayBuffer

/**
 * Created by zx on 16-1-8.
 *
 * This Scheduler is based on Memory-Usage-Rate, and running on executor.
 */
class MURScheduler(
     executorId: String, conf: SparkConf, env: SparkEnv) extends Serializable with Logging {

  // the second value of runningTasks save the taskType(shuffle, result) of this task
  private val runningTasks = new ConcurrentHashMap[Long, Int]
  private val finishedTasks = new ArrayBuffer[Long]()

  private val mursRecommendStopTasks = new ConcurrentHashMap[Int, Long]
  private val stopIndex = 1
  private val mursStopTasks = new ConcurrentHashMap[Long, Int]

  private val runningTasksSampleFlag = new ConcurrentHashMap[Long, Boolean]

  val taskMURSample = new MURSchedulerSample

  /**
   * Show sample message of one task
   *
   */

  def showMessage(taskId: Long): Unit = {

    if( (! runningTasks.containsKey(taskId)) || (taskId % 4 != 0) )
      return

    val bytesRead_input = taskMURSample.getBytesReadInput(taskId)
    val bytesRead_shuffle = taskMURSample.getBytesReadShuffle(taskId)

    val totalRecords = taskMURSample.getTotalRecords(taskId)
    val recordsRead_input = taskMURSample.getRecordsReadInput(taskId)
    val recordsRead_shuffle = taskMURSample.getRecordsReadShuffle(taskId)
    val recordsRead_cache = taskMURSample.getRecordsReadCache(taskId)
    val recordsRead_total = taskMURSample.getRecordsReadCogroup(taskId)

    val bytesOutput = taskMURSample.getBytesOutput(taskId)
    val bytesShuffleWrite = taskMURSample.getBytesShuffleWrite(taskId)

    val shuffleMemoryUsage = taskMURSample.getShuffleMemoryUsage(taskId)
    val cacheMemoryUsage = taskMURSample.getCacheMemoryUsage(taskId)
    logInfo(s"Task $taskId has bytes read $bytesRead_input/$bytesRead_shuffle, " +
      s"records $totalRecords, read records $recordsRead_input/$recordsRead_shuffle/$recordsRead_cache/$recordsRead_total, " +
      s"bytes output $bytesOutput, shuffle write $bytesShuffleWrite, " +
      s"memory usage $shuffleMemoryUsage/$cacheMemoryUsage.")
  }

  def registerTask(taskId: Long): Unit = {
    runningTasks.put(taskId, 0)
    runningTasksSampleFlag.put(taskId, false)
    taskMURSample.registerTask(taskId)
  }

  def removeFinishedTask(taskId: Long): Unit = {
    removeStopTask()
    runningTasks.remove(taskId)
    runningTasksSampleFlag.remove(taskId)
    taskMURSample.removeFinishedTask(taskId)
  }

  /**
   *  1. Executor use [updateAllSampleFlag] to update the sample flag of all running tasks.
   *  2. While the running tasks use [getSampleFlag] to decide whether they should do sample
   *  and report the results.
   *  3. After they report the results, they use [updateSingleTaskSampleFlag] to tell
   *  the Scheduler they have finish themselves.
   *
   */

  def getSampleFlag(taskId: Long): Boolean = {
    runningTasksSampleFlag.get(taskId)
  }

  def updateAllSampleFlag(): Unit = {
    val keyIter = runningTasksSampleFlag.keySet.iterator()
    while(keyIter.hasNext)
      runningTasksSampleFlag.replace(keyIter.next(), true)
  }

  def updateSingleTaskSampleFlag(taskId: Long): Unit = {
    runningTasksSampleFlag.replace(taskId, false)
  }

  /**
   * All these update* functions are used to invoke the function of taskMURSample
   * because all result are stored in the taskMURSample.
   */

  // Before the task read it's input, we update the total records first
  def updateTotalRecords(taskId: Long, totalRecords: Long) =
    taskMURSample.updateTotalRecords(taskId, totalRecords)

  // this method will only be used in cache operation
  def updateReadRecordsInCache(taskId: Long, readRecords: Long) =
    taskMURSample.updateReadRecordsInCache(taskId, readRecords)

  // this method will only be used in cogroup operation
  def updateReadRecordsInCoCroup(taskId: Long, readRecords: Long) =
    taskMURSample.updateReadRecordsInCoCroup(taskId, readRecords)

  def updateShuffleSampleResult(taskId: Long, sampleResult: Long): Unit = {
    updateSingleTaskSampleFlag(taskId)
    taskMURSample.updateShuffleSampleResult(taskId, sampleResult)
  }

  def updateCacheSampleResult(taskId: Long, sampleResult: Long): Unit = {
    updateSingleTaskSampleFlag(taskId)
    taskMURSample.updateCacheSampleResult(taskId, sampleResult)
  }

  def updateTaskInformation(taskId: Long, taskMetrics: TaskMetrics): Unit = {
    taskMURSample.updateTaskInformation(taskId, taskMetrics)
  }

  /**
   * Scheduler Implementation
   *
   */

  def addStopTask(taskId: Long): Unit ={
    mursStopTasks.put(taskId, stopIndex)
  }

  def removeStopTask(taskId: Long): Unit ={
    mursStopTasks.remove(taskId)
  }

  def removeStopTask(): Unit ={
    logInfo("Remove all stop tasks.")
    mursStopTasks.clear()
  }

  def shouldStop(taskId: Long): Boolean = mursStopTasks.containsKey(taskId)

  def hasStopTask(): Boolean = !mursStopTasks.isEmpty

  def addRecommendStopTask(taskId: Long, stopLevel: Int): Unit = {
    mursRecommendStopTasks.put(stopLevel, taskId)
    addStopTask(taskId)
  }

  def computeStopTask(): Unit ={
    val (tasks, totalRecords) = taskMURSample.getAllTotalRecordsRead()
    val inputRecords = taskMURSample.getAllRecordsRead()
    val inputBytes = taskMURSample.getAllBytesRead()
    val memoryUsage = taskMURSample.getAllMemoryUsage()
    val memoryManager = env.memoryManager
    val totalMemory = conf.getSizeAsBytes("spark.executor.memory")
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.75)
    val sum = memoryManager.executionMemoryUsed + memoryManager.storageMemoryUsed
    val yellowLine = conf.getDouble("spark.murs.yellow", 0.4)
    val yellowMemoryUsage = (totalMemory * memoryFraction * yellowLine).toLong
    //if(sum > yellowMemoryUsage && !hasStopTask()){
    if(!hasStopTask()){
      logInfo(s"Memory pressure must be optimized.($sum/$yellowMemoryUsage/$totalMemory)")
      var minMemoryUsageIndex = 0
      for (i <- 0 until tasks.length) {
        if (memoryUsage(i) < memoryUsage(minMemoryUsageIndex))
          minMemoryUsageIndex = i
      }
      val recommandStopTask = tasks(minMemoryUsageIndex)
      if(runningTasks.containsKey(recommandStopTask)){
        addStopTask(tasks(minMemoryUsageIndex))
      }
    }
  }

}
