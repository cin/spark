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
    val policy = PreemptionPolicy.mkPolicy(conf, executorIds, removeTimes, preemptedExecutors)

    policy should not be null
    val ypm = PreemptExecutors(Set.empty, Set.empty, 0)
    policy.preemptExecutors(ypm)
    preemptedExecutors should have size 0
  }

  test("preemption selection with forced removal") {
    sc.executorAllocationManager shouldBe defined
    val eam = sc.executorAllocationManager.get
    val numExecs = 2
    eam.executorIds ++= scala.util.Random.shuffle((0 until 4).map(_.toString))
    val rmex = scala.util.Random.shuffle(eam.executorIds).take(numExecs).toSet
    val ypm = PreemptExecutors(rmex, Set.empty, 0)
    eam.preemptExecutors(ypm)
    eam.preemptableExecutors should have size numExecs
    eam.preemptableExecutors.foreach { pe =>
      rmex should contain (pe)
    }
  }

  test("preemption selection with asked removal and no idle execs") {
    sc.executorAllocationManager shouldBe defined
    val eam = sc.executorAllocationManager.get
    val numExecs = 1
    eam.executorIds ++= scala.util.Random.shuffle((0 until 4).map(_.toString))
    val rmex = scala.util.Random.shuffle(eam.executorIds).take(numExecs).toSet
    val ypm = PreemptExecutors(Set.empty, rmex, numExecs)
    eam.preemptExecutors(ypm)
    eam.preemptableExecutors should have size numExecs
    eam.preemptableExecutors.foreach { pe =>
      rmex should contain (pe)
    }
  }

  test("preemption selection with asked removal and idle execs") {
    sc.executorAllocationManager shouldBe defined
    val eam = sc.executorAllocationManager.get
    val numExecs = 1
    eam.executorIds ++= scala.util.Random.shuffle((0 until 4).map(_.toString))
    val head = eam.executorIds.head
    eam.removeTimes(eam.executorIds.takeRight(1).head) = System.currentTimeMillis + 1000L
    val rmex = scala.util.Random.shuffle(eam.executorIds).take(numExecs).toSet
    val ypm = PreemptExecutors(Set.empty, rmex, 2)
    eam.preemptExecutors(ypm)
    eam.preemptableExecutors should have size 2
    eam.preemptableExecutors should contain (head)
    eam.preemptableExecutors should contain (rmex.head)
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

    val ypm = PreemptExecutors(Set.empty, Set.empty, numExecs)
    eam.preemptExecutors(ypm)
    eam.preemptableExecutors should have size numExecs
    eam.preemptableExecutors.foreach { eid =>
      idleExecs should contain (eid)
    }
  }

  test("test ordering") {
    val executorIds = new mutable.HashSet[String]
    val removeTimes = new mutable.HashMap[String, Long]
    val preemptedExecutors = new mutable.HashSet[String]

    val conf = new SparkConf()
      .setMaster("myDummyLocalExternalClusterManager")
      .setAppName("test-executor-allocation-manager")
    val policy = PreemptionPolicy
      .mkPolicy(conf, executorIds, removeTimes, preemptedExecutors)
      .asInstanceOf[DefaultPreemptionPolicy]
    val execMap = new mutable.HashMap[String, Long]
    execMap += "1" -> 13L
    execMap += "2" -> 11L
    execMap += "3" -> 10L
    execMap += "4" -> 12L
    execMap += "5" -> 9L
    execMap += "6" -> 8L

    val res = policy.orderByPreemtableness(execMap, 4)
    res should have size 4
    res shouldBe Seq("6", "5", "3", "2")
  }
}
