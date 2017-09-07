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

import org.apache.spark.internal.config.DYN_ALLOCATION_PREEMPTION_POLICY
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
        classOf[mutable.HashSet[String]]
      )
  }

  def mkPolicy(
      conf: SparkConf,
      executorIds: mutable.HashSet[String],
      removeTimes: mutable.HashMap[String, Long],
      preemptedExecutors: mutable.HashSet[String]): PreemptionPolicy = {
    getCtor(conf)
      .newInstance(executorIds, removeTimes, preemptedExecutors)
      .asInstanceOf[PreemptionPolicy]
  }
}

/**
 * The preemptExecutors method is synchronized in the ExecutorAllocationManager (EAM),
 * so it's not necessary here. But still...beware.
 *
 * @param executorIds mutable collection of executorIds (not mutatated by default)
 * @param removeTimes EAM's idle executor -> expiration map (not mutated by default)
 * @param preemptedExecutors mutable collection of executors to be preempted on the next
 *                           EAM update. preemptExecutors adds to this set, but EAM acts
 *                           on and clears the set
 */
abstract class PreemptionPolicy(
    executorIds: mutable.HashSet[String],
    removeTimes: mutable.HashMap[String, Long],
    preemptedExecutors: mutable.HashSet[String]) {
  def preemptExecutors(ypm: PreemptExecutors): Unit
}

/**
 * DefaultPreemptionPolicy is the default preemption policy selected when
 * DYN_ALLOCATION_PREEMPTION_POLICY is not set. This policy was created with YARN's
 * PreemptionMessage symantics in mind. Instead of simply killing YARN's requested
 * containers, preemptively remove idle executors without cached data. If YARN requests
 * to preempt more executors than are idle, include the `askedToLeave` set and randomly
 * select executors to fill out the total preemption count.
 */
class DefaultPreemptionPolicy(
    executorIds: mutable.HashSet[String],
    removeTimes: mutable.HashMap[String, Long],
    preemptedExecutors: mutable.HashSet[String]) extends
  PreemptionPolicy(executorIds, removeTimes, preemptedExecutors) {

  protected[spark] def orderByPreemtableness(
      execMap: mutable.HashMap[String, Long],
      numExecs: Int): Seq[String] = {
    execMap.toSeq.sortBy(_._2).take(numExecs).map(_._1)
  }

  /**
   * find executors to offer up for preemption
   * favor most idle executors without cached data
   * NOTE: does not consider non-idle executors
   */
  protected[spark] def findPreemptableExecutors(numExecsToKill: Int): Seq[String] = {
    val blockMaster = SparkEnv.get.blockManager.master
    val (execsWithCachedData, execsWithoutCachedData) = removeTimes
      .partition { case (id, _) => blockMaster.hasCachedBlocks(id) }
    if (execsWithoutCachedData.size < numExecsToKill) {
      val execsLeftToKill = numExecsToKill - execsWithoutCachedData.size
      execsWithoutCachedData.keys.toSeq ++
        orderByPreemtableness(execsWithCachedData, execsLeftToKill)
    } else orderByPreemtableness(execsWithoutCachedData, numExecsToKill)
  }

  override def preemptExecutors(ypm: PreemptExecutors): Unit = {
    val execsAskedToLeave = ypm.numRequestedContainers
    val numExecsToPreempt = ypm.forcedToLeave.size + execsAskedToLeave
    val execsToPreempt = mutable.LinkedHashSet(ypm.forcedToLeave.toSeq: _*)
    if (execsAskedToLeave > 0) {
      execsToPreempt ++= findPreemptableExecutors(execsAskedToLeave)
      execsToPreempt ++= ypm.askedToLeave
      val remaining = numExecsToPreempt - execsToPreempt.size
      if (remaining > 0) execsToPreempt ++= scala.util.Random.shuffle(executorIds).take(remaining)
    }

    if (numExecsToPreempt > 0 && execsToPreempt.nonEmpty) {
      preemptedExecutors ++= execsToPreempt.take(numExecsToPreempt)
    }
  }
}
