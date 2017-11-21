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

package org.apache.spark

import java.lang.reflect.Constructor

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{
  DYN_ALLOCATION_PREEMPTION_POLICY, DYN_ALLOCATION_PREEMPTION_POLICY_ENABLED}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.PreemptExecutors
import org.apache.spark.util.Utils

/**
 * PreemptionPolicy companion object abstracts loading the
 * `DYN_ALLOCATION_PREEMPTION_POLICY`.
 */
object PreemptionPolicy {
  private def getCtor(conf: SparkConf): Constructor[_] = {
    Utils
      .classForName(conf.get(DYN_ALLOCATION_PREEMPTION_POLICY))
      .getConstructor(
        classOf[mutable.HashSet[String]],
        classOf[mutable.HashMap[String, Long]],
        classOf[(Seq[String], Boolean) => Seq[String]]
      )
  }

  def mkPolicy(
      conf: SparkConf,
      executorIds: mutable.HashSet[String],
      removeTimes: mutable.HashMap[String, Long],
      removeExecutorsFn: (Seq[String], Boolean) => Seq[String]): Option[PreemptionPolicy] = {
    if (conf.get(DYN_ALLOCATION_PREEMPTION_POLICY_ENABLED)) {
      Some(getCtor(conf)
        .newInstance(executorIds, removeTimes, removeExecutorsFn)
        .asInstanceOf[PreemptionPolicy])
    } else None
  }
}

/**
 * The preemptExecutors method is synchronized in the ExecutorAllocationManager (EAM),
 * so it's not necessary here. But still...beware.
 *
 * @param executorIds mutable collection of executorIds (not mutated by default)
 * @param removeTimes EAM's idle executor -> expiration map (not mutated by default)
 */
abstract class PreemptionPolicy(
    executorIds: mutable.HashSet[String],
    removeTimes: mutable.HashMap[String, Long],
    removeExecutorsFn: (Seq[String], Boolean) => Seq[String]) extends Logging {

  protected[spark] val executorsToPreempt = new mutable.HashSet[String]

  def clearExecutorsToPreempt(): Unit = synchronized {
    executorsToPreempt.clear()
  }

  def preemptExecutors(): Unit = synchronized {
    logInfo(s"Preempting executors $executorsToPreempt")
    removeExecutorsFn(executorsToPreempt.toSeq, true)
    executorsToPreempt.clear()
  }

  def legislate(pe: PreemptExecutors): Unit
}

/**
 * NoPreemptionPolicy doesn't do anything. Mainly for testing.
 */
class NoPreemptionPolicy(
    executorIds: mutable.HashSet[String],
    removeTimes: mutable.HashMap[String, Long],
    removeExecutorsFn: (Seq[String], Boolean) => Seq[String]) extends
  PreemptionPolicy(executorIds, removeTimes, removeExecutorsFn) {

  override def legislate(pe: PreemptExecutors): Unit = {}
}

/**
 * SimplePreemptionPolicy simply sets [[PreemptionPolicy.executorsToPreempt]] to the set of
 * executors specifed by the PreemptExecutors message.
 */
class SimplePreemptionPolicy(
    executorIds: mutable.HashSet[String],
    removeTimes: mutable.HashMap[String, Long],
    removeExecutorsFn: (Seq[String], Boolean) => Seq[String]) extends
  PreemptionPolicy(executorIds, removeTimes, removeExecutorsFn) {

  override def legislate(pe: PreemptExecutors): Unit = synchronized {
    executorsToPreempt.clear()
    if (pe.forcedToLeave.nonEmpty) executorsToPreempt ++= pe.forcedToLeave
    if (pe.askedToLeave.nonEmpty) executorsToPreempt ++= pe.askedToLeave
  }
}

/**
 * DefaultPreemptionPolicy is the default preemption policy selected when
 * DYN_ALLOCATION_PREEMPTION_POLICY is not set. This policy was created with YARN's
 * PreemptionMessage symantics in mind. Instead of simply killing YARN's requested
 * containers, the application master can chose another set of executors to preempt.
 * In this case, the default policy sorts the executors by whether they have cached
 * data and their [[removeTimes]] if possible. If an executor is not idle, set it to
 * Long.MaxValue so it will be toward the end of the list.
 *
 * It is also important to note that other resource managers may have specific requirements
 * to respond to their method of preemption. YARN does not have this requirement.
 */
class DefaultPreemptionPolicy(
    executorIds: mutable.HashSet[String],
    removeTimes: mutable.HashMap[String, Long],
    removeExecutorsFn: (Seq[String], Boolean) => Seq[String]) extends
  PreemptionPolicy(executorIds, removeTimes, removeExecutorsFn) {

  private def sortExecutorsByPreemptableness(pe: PreemptExecutors): Seq[String] = {
    val blockMaster = SparkEnv.get.blockManager.master
    executorIds.flatMap {
      case id if pe.forcedToLeave.contains(id) => None
      case id =>
        Some((blockMaster.hasCachedBlocks(id), removeTimes.getOrElse(id, Long.MaxValue), id))
    }.toSeq.sorted.map(_._3)
  }

  override def legislate(pe: PreemptExecutors): Unit = synchronized {
    executorsToPreempt.clear() // this should be empty but enforce it
    if (pe.forcedToLeave.nonEmpty) executorsToPreempt ++= pe.forcedToLeave
    val numExecutorsAskedToLeave = pe.askedToLeave.size
    if (numExecutorsAskedToLeave > 0) {
      executorsToPreempt ++= sortExecutorsByPreemptableness(pe).take(numExecutorsAskedToLeave)
    }
  }
}
