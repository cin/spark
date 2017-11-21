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

import scala.collection.mutable

import org.scalatest.Matchers

import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.PreemptExecutors

class PreemptionPolicySuite extends SparkFunSuite
  with LocalSparkContext with Matchers {

  override def beforeEach(): Unit = sc = mkSparkContext()

  private def mkSparkContext(
    minExecutors: Int = 1,
    maxExecutors: Int = 4,
    initialExecutors: Int = 1): SparkContext = {
    val conf = new SparkConf()
      .setMaster("myDummyLocalExternalClusterManager")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", minExecutors.toString)
      .set("spark.dynamicAllocation.maxExecutors", maxExecutors.toString)
      .set("spark.dynamicAllocation.initialExecutors", initialExecutors.toString)
      .set("spark.dynamicAllocation.testing", "true")
    new SparkContext(conf)
  }

  test("reflect to get the default policy") {
    val executorIds = new mutable.HashSet[String]
    val removeTimes = new mutable.HashMap[String, Long]
    val preemptedExecutors = new mutable.HashSet[String]

    val conf = new SparkConf()
      .setMaster("myDummyLocalExternalClusterManager")
      .setAppName("test-executor-allocation-manager")
    val policy = PreemptionPolicy.mkPolicy(conf, executorIds, removeTimes,
      sc.executorAllocationManager.get.removeExecutors).get

    policy should not be null
    val pe = PreemptExecutors(Set.empty, Set.empty, 0)
    policy.legislate(pe)
    preemptedExecutors should have size 0
  }

  test("preemption selection with forced removal") {
    sc.executorAllocationManager shouldBe defined
    val eam = sc.executorAllocationManager.get
    val numExecs = 2
    eam.executorIds ++= scala.util.Random.shuffle((0 until 4).map(_.toString))
    val rmex = scala.util.Random.shuffle(eam.executorIds).take(numExecs).toSet
    val pe = PreemptExecutors(rmex, Set.empty, 0)
    eam.handlePreemptExecutorsMessage(pe)
    eam.preemptionPolicy.get.executorsToPreempt should have size numExecs
    eam.preemptionPolicy.get.executorsToPreempt.foreach { pe =>
      rmex should contain (pe)
    }
  }

  test("preemption selection with asked removal and no idle execs") {
    sc.executorAllocationManager shouldBe defined
    val eam = sc.executorAllocationManager.get
    val numExecs = 1
    eam.executorIds ++= scala.util.Random.shuffle((0 until 4).map(_.toString))
    // the default policy will sort executors for removal based on cached data, idle time, and id
    // in this case, since the executors have no cached data and are not idle, the id will be used
    val rmex = eam.executorIds.toList.sorted.take(numExecs).toSet
    val pem = PreemptExecutors(Set.empty, rmex, numExecs)
    eam.handlePreemptExecutorsMessage(pem)
    eam.preemptionPolicy.get.executorsToPreempt should have size numExecs
    eam.preemptionPolicy.get.executorsToPreempt.foreach { pe =>
      rmex should contain (pe)
    }
  }

  test("preemption selection with asked removal and idle execs") {
    sc.executorAllocationManager shouldBe defined
    val eam = sc.executorAllocationManager.get
    val numExecs = 1
    eam.executorIds ++= scala.util.Random.shuffle((0 until 4).map(_.toString))
    val expected1 = eam.executorIds.takeRight(1).head
    val expected2 = eam.executorIds.toList.filterNot(_ == expected1).min
    eam.removeTimes(expected1) = System.currentTimeMillis + 1000L
    val rmex = scala.util.Random.shuffle(eam.executorIds).take(numExecs).toSet
    val pe = PreemptExecutors(Set.empty, rmex, 2)
    eam.handlePreemptExecutorsMessage(pe)
    eam.preemptionPolicy.get.executorsToPreempt should have size 2
    eam.preemptionPolicy.get.executorsToPreempt shouldBe Set(expected1, expected2)
  }


  test("preemption selection with asked removal and multiple idle execs") {
    sc.executorAllocationManager shouldBe defined
    val eam = sc.executorAllocationManager.get
    val numExecs = 3
    eam.executorIds ++= scala.util.Random.shuffle((0 until 4).map(_.toString))
    val expected1 = eam.executorIds.head
    val expected2 = eam.executorIds.toList.filterNot(_ == expected1).min
    val expected3 = eam.executorIds.toList.filterNot(Set(expected1, expected2).contains).min
    eam.removeTimes(expected2) = System.currentTimeMillis + 2000L
    eam.removeTimes(expected3) = System.currentTimeMillis + 1000L
    val rmex = scala.util.Random.shuffle(eam.executorIds).take(numExecs).toSet
    val pe = PreemptExecutors(Set.empty, rmex, 3)
    eam.handlePreemptExecutorsMessage(pe)
    eam.preemptionPolicy.get.executorsToPreempt should have size 3
    eam.preemptionPolicy.get.executorsToPreempt should contain (expected2)
    eam.preemptionPolicy.get.executorsToPreempt should contain (expected3)
    val expected4 = eam.executorIds.toList.filterNot(Set(expected2, expected3).contains).min
    eam.preemptionPolicy.get.executorsToPreempt should contain (expected4)
  }

  test("preemption selection with no asked removal and idle execs") {
    sc.executorAllocationManager shouldBe defined
    val eam = sc.executorAllocationManager.get
    val numExecs = 2
    eam.executorIds ++= scala.util.Random.shuffle((0 until 4).map(_.toString))
    val idleExecs = new mutable.HashSet[String]
    eam.executorIds.takeRight(numExecs).foreach { eid =>
      eam.removeTimes(eid) = System.currentTimeMillis + 1000L
      idleExecs.add(eid)
    }

    val pe = PreemptExecutors(Set.empty, Set.empty, numExecs)
    eam.handlePreemptExecutorsMessage(pe)
    eam.preemptionPolicy.get.executorsToPreempt should have size numExecs
    eam.preemptionPolicy.get.executorsToPreempt.foreach { eid =>
      idleExecs should contain (eid)
    }
  }
}
