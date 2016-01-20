package org.apache.spark

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.MURScheduler

/**
 * Created by zx on 16-1-12.
 */
private[spark] class TaskContextMURSImpl (
  override val stageId: Int,
  override val partitionId: Int,
  override val taskAttemptId: Long,
  override val attemptNumber: Int,
  override val taskMemoryManager: TaskMemoryManager,
  @transient private val metricsSystem: MetricsSystem,
  internalAccumulators: Seq[Accumulator[Long]],
  override val taskMURS: MURScheduler,
  override val runningLocally: Boolean = false,
  override val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContextImpl(stageId,
    partitionId,
    taskAttemptId,
    attemptNumber,
    taskMemoryManager,
    metricsSystem,
    internalAccumulators,
    runningLocally,
    taskMetrics)
  with Logging {

  override def isMURStop() = taskMURS.shouldStop(taskAttemptId)

  }

