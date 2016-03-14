package org.apache.spark.scheduler

import java.util
import java.util.concurrent.ConcurrentHashMap
import  java.util.TreeMap

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
  private val runningTasksMemoryManage = new ConcurrentHashMap[Long, TaskMemoryManager]
  private val finishedTasks = new ArrayBuffer[Long]()

  private var reStartIndex = 0
  private val mursRecommendStopTasks = new ConcurrentHashMap[Int, Long]
  private var stopIndex = 0
  private val mursStopTasks = new ConcurrentHashMap[Int, Long]()

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

  def registerTask(taskId: Long, taskMemoryManager: TaskMemoryManager): Unit = {
    runningTasks.put(taskId, 0)
    runningTasksMemoryManage.put(taskId, taskMemoryManager)
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

  def addStopTasks(): Unit = {
    for(i <- 0 until stopIndex){
      if(mursRecommendStopTasks.containsKey(i)) {
        logInfo("Add stop task: " + mursRecommendStopTasks.get(i))
        mursStopTasks.put(i, mursRecommendStopTasks.get(i))
        mursRecommendStopTasks.remove(i)
      }
    }
  }

  def removeStopTask(): Unit ={
    if(mursStopTasks.containsKey(reStartIndex)) {
      logInfo("Remove stop task: " + mursStopTasks.get(reStartIndex))
      mursStopTasks.remove(reStartIndex)
      reStartIndex += 1
    }
  }

  def shouldStop(taskId: Long): Boolean = mursStopTasks.contains(taskId)

  def hasStopTask(): Boolean = !mursStopTasks.isEmpty

  def addRecommendStopTask(taskId: Long, index: Int): Unit = {
    mursRecommendStopTasks.put(index, taskId)
  }

  private var totalOldMemory: Long = 0
  private var yellowMemoryUsage: Long = 0

  def updateMemroyLine(totalOld: Long, yellowLine: Long): Unit = {
    totalOldMemory = totalOld
    yellowMemoryUsage = yellowLine
  }

  def computeStopTask(): Unit ={
    val memoryManager = env.memoryManager
    if(memoryManager.shouldStopTasks() && !mursRecommendStopTasks.isEmpty){
      addStopTasks()
      memoryManager.registerHasStop()
    }
    val usedMemory = memoryManager.executionMemoryUsed + memoryManager.storageMemoryUsed
    val freeMemory = totalOldMemory - memoryManager.executionMemoryUsed - memoryManager.storageMemoryUsed

    //val coreNum=conf.getInt("spark.executor.cores",12)
    if(!hasStopTask() && usedMemory > yellowMemoryUsage){
      logInfo(s"Memory pressure must be optimized.($usedMemory/$yellowMemoryUsage/$freeMemory)")
      memoryManager.registerStop()
      val runningTasksArray = taskMURSample.getTasks()
      val tasksMemoryConsumption = runningTasksArray.map(taskId => {
        val taskMemoryManger = runningTasksMemoryManage.get(taskId)
        taskMemoryManger.getMemoryConsumptionForThisTask
      })
      var testFreeMemory = freeMemory
      for(i <- 0 until runningTasksArray.length){
        if(testFreeMemory - tasksMemoryConsumption(i) < 0){
          addRecommendStopTask(runningTasksArray(i), stopIndex)
          stopIndex += 1
        }
        testFreeMemory -= tasksMemoryConsumption(i)
      }

//      val tasks = taskMURSample.getTasks()
//      for(i <- 0 until tasks.length){
//        //showMessage(tasks(i))
//        if(tasks(i) % 12 == 0) {
//          val taskMemoryManager = runningTasksMemoryManage.get(tasks(i))
//          logInfo("memory usage : " + tasks(i) + "---" + taskMemoryManager.getMemoryConsumptionForThisTask)
//          showMessage(tasks(i))
//        }
//      }
     // val(tasks, totalRecords) = taskMURSample.getAllTotalRecordsRead()
      //val deltaInputRecords = taskMURSample.getAllRecordsReadDeltaValue()
     // val inputRecords = taskMURSample.getAllRecordsRead()
      //val inputBytes = taskMURSample.getAllBytesRead()
     // val deltaMemoryUsage = taskMURSample.getAllMemoryUsageDeltaValue()
      //val memoryUsage = taskMURSample.getAllMemoryUsage()
      /*
      var index = 0
      val length = tasks.length
      while (freeMemory > 0 && index < length ){
        val needMemory = ((memoryUsage(index)):(Double)) *(
          ((totalRecords(index)):(Double)) - ((inputRecords(index)):(Double)) )/ ((inputRecords(index)):(Double))
        if(freeMemory > needMemory){
          index += 1
        }
        freeMemory -=  needMemory
      }
      logInfo(s"the number of the tasks we decide  to run is $index + 1")
      for (i <- index until tasks.length){
        val recommandStopTask = tasks(i)
        if( runningTasks.containsKey( recommandStopTask)){
          addStopTask(recommandStopTask)
        }
      }
      val MURTreeMap = new util.TreeMap[Double,Long]()
      //var maxMemoryUsageRationIndex = 0
      for(i <- 0 until tasks.length ){
        MURTreeMap.put( memoryUseRatio(deltaMemoryUsage,inputBytes,deltaInputRecords,totalRecords,i),tasks(i))
        }
      while (MURTreeMap.size()> coreNum){
        val highestMUR= MURTreeMap.lastKey()
        val recommandStopTask = MURTreeMap.get(highestMUR)
        if( runningTasks.containsKey( recommandStopTask)){
          addStopTask(recommandStopTask)
        }
        MURTreeMap.remove(highestMUR)
      }
      */
    }

    def memoryUseRatio(memoryUsage: Array[Long], inputBytes: Array[Long], inputRecords: Array[Long], totalRecords: Array[Long], index: Int):Double={
      val mur:Double =( memoryUsage(index): (Double) ) / ( (inputBytes(index): (Double)) *
        ( inputRecords(index): (Double)) / (totalRecords(index): (Double)) )
      mur
    }

  }

}
