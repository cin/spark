/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.ExecutorLossReason
import org.apache.spark.util.SerializableBuffer

private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable

private[spark] object CoarseGrainedClusterMessages {

  case object RetrieveSparkAppConfig extends CoarseGrainedClusterMessage

  case class SparkAppConfig(
      sparkProperties: Seq[(String, String)],
      ioEncryptionKey: Option[Array[Byte]],
      hadoopDelegationCreds: Option[Array[Byte]])
    extends CoarseGrainedClusterMessage

  case object RetrieveLastAllocatedExecutorId extends CoarseGrainedClusterMessage

  // Driver to executors
  case class LaunchTask(data: SerializableBuffer) extends CoarseGrainedClusterMessage

  case class KillTask(taskId: Long, executor: String, interruptThread: Boolean, reason: String)
    extends CoarseGrainedClusterMessage

  case class KillExecutorsOnHost(host: String)
    extends CoarseGrainedClusterMessage

  sealed trait RegisterExecutorResponse

  case object RegisteredExecutor extends CoarseGrainedClusterMessage with RegisterExecutorResponse

  case class RegisterExecutorFailed(message: String) extends CoarseGrainedClusterMessage
    with RegisterExecutorResponse

  // Executors to driver
  case class RegisterExecutor(
      executorId: String,
      executorRef: RpcEndpointRef,
      hostname: String,
      cores: Int,
      logUrls: Map[String, String])
    extends CoarseGrainedClusterMessage

  case class StatusUpdate(executorId: String, taskId: Long, state: TaskState,
    data: SerializableBuffer) extends CoarseGrainedClusterMessage

  object StatusUpdate {
    /** Alternate factory method that takes a ByteBuffer directly for the data field */
    def apply(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer)
      : StatusUpdate = {
      StatusUpdate(executorId, taskId, state, new SerializableBuffer(data))
    }
  }

  // Internal messages in driver
  case object ReviveOffers extends CoarseGrainedClusterMessage

  case object StopDriver extends CoarseGrainedClusterMessage

  case object StopExecutor extends CoarseGrainedClusterMessage

  case object StopExecutors extends CoarseGrainedClusterMessage

  case class RemoveExecutor(executorId: String, reason: ExecutorLossReason)
    extends CoarseGrainedClusterMessage

  case class RemoveWorker(workerId: String, host: String, message: String)
    extends CoarseGrainedClusterMessage

  case class SetupDriver(driver: RpcEndpointRef) extends CoarseGrainedClusterMessage

  // Exchanged between the driver and the AM in Yarn client mode
  case class AddWebUIFilter(
      filterName: String, filterParams: Map[String, String], proxyBase: String)
    extends CoarseGrainedClusterMessage

  // Messages exchanged between the driver and the cluster manager for executor allocation
  // In Yarn mode, these are exchanged between the driver and the AM

  case class RegisterClusterManager(am: RpcEndpointRef) extends CoarseGrainedClusterMessage

  // Request executors by specifying the new total number of executors desired
  // This includes executors already pending or running
  case class RequestExecutors(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int],
      nodeBlacklist: Set[String])
    extends CoarseGrainedClusterMessage

  // Check if an executor was force-killed but for a reason unrelated to the running tasks.
  // This could be the case if the executor is preempted, for instance.
  case class GetExecutorLossReason(executorId: String) extends CoarseGrainedClusterMessage

  case class KillExecutors(executorIds: Seq[String]) extends CoarseGrainedClusterMessage

  /**
   * Abstraction of YARN's preemption message that can be used by other RM clients as well.
   *
   * @param forcedToLeave set of executors that should forcefully be removed. This is YARN
   *                      specific but could be used by other RM clients to ensure this set
   *                      of executors is removed.
   * @param askedToLeave set of executors that the RM is requesting to be removed. In the case of
   *                     YARN, the AM does not have to respect this set and can choose to preempt
   *                     any set of executors it sees fit (as long as it matches the Resource
   *                     Request, which in Spark's case are all executors because they all share
   *                     the same CPU/memory settings.
   */
  case class PreemptExecutors(
      forcedToLeave: Set[String],
      askedToLeave: Set[String])
    extends CoarseGrainedClusterMessage

  // Used internally by executors to shut themselves down.
  case object Shutdown extends CoarseGrainedClusterMessage

}
