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

package org.apache.spark.deploy.yarn

import java.util.{List => JList, Set => JSet}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.YarnAllocator._
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.util.ManualClock

class MockResolver extends SparkRackResolver {

  override def resolve(conf: Configuration, hostName: String): String = {
    if (hostName == "host3") "/rack2" else "/rack1"
  }

}

class YarnAllocatorSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach {
  val conf = new YarnConfiguration()
  val sparkConf = new SparkConf()
  sparkConf.set("spark.driver.host", "localhost")
  sparkConf.set("spark.driver.port", "4040")
  sparkConf.set(SPARK_JARS, Seq("notarealjar.jar"))
  sparkConf.set("spark.yarn.launchContainers", "false")

  val appAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0)

  // Resource returned by YARN.  YARN can give larger containers than requested, so give 6 cores
  // instead of the 5 requested and 3 GB instead of the 2 requested.
  val containerResource = Resource.newInstance(3072, 6)

  var rmClient: AMRMClient[ContainerRequest] = _

  var containerNum = 0

  override def beforeEach() {
    super.beforeEach()
    rmClient = AMRMClient.createAMRMClient()
    rmClient.init(conf)
    rmClient.start()
  }

  override def afterEach() {
    try {
      rmClient.stop()
    } finally {
      super.afterEach()
    }
  }

  class MockSplitInfo(host: String) extends SplitInfo(null, host, null, 1, null) {
    override def hashCode(): Int = 0
    override def equals(other: Any): Boolean = false
  }

  def createAllocator(
      maxExecutors: Int = 5,
      rmClient: AMRMClient[ContainerRequest] = rmClient): YarnAllocator = {
    val args = Array(
      "--jar", "somejar.jar",
      "--class", "SomeClass")
    val sparkConfClone = sparkConf.clone()
    sparkConfClone
      .set("spark.executor.instances", maxExecutors.toString)
      .set("spark.executor.cores", "5")
      .set("spark.executor.memory", "2048")
    new YarnAllocator(
      "not used",
      mock(classOf[RpcEndpointRef]),
      conf,
      sparkConfClone,
      rmClient,
      appAttemptId,
      new SecurityManager(sparkConf),
      Map(),
      new MockResolver())
  }

  def createContainer(host: String): Container = {
    // When YARN 2.6+ is required, avoid deprecation by using version with long second arg
    val containerId = ContainerId.newInstance(appAttemptId, containerNum)
    containerNum += 1
    val nodeId = NodeId.newInstance(host, 1000)
    Container.newInstance(containerId, nodeId, "", containerResource, RM_REQUEST_PRIORITY, null)
  }

  test("single container allocated") {
    // request a single container and receive it
    val handler = createAllocator(1)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (1)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container.getId)

    val size = rmClient.getMatchingRequests(container.getPriority, "host1", containerResource).size
    size should be (0)
  }

  test("container should not be created if requested number if met") {
    // request a single container and receive it
    val handler = createAllocator(1)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (1)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container.getId)

    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container2))
    handler.getNumExecutorsRunning should be (1)
  }

  test("some containers allocated") {
    // request a few containers and receive some of them
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host1")
    val container3 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2, container3))

    handler.getNumExecutorsRunning should be (3)
    handler.allocatedContainerToHostMap.get(container1.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container3.getId).get should be ("host2")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container1.getId)
    handler.allocatedHostToContainersMap.get("host1").get should contain (container2.getId)
    handler.allocatedHostToContainersMap.get("host2").get should contain (container3.getId)
  }

  test("receive more containers than requested") {
    val handler = createAllocator(2)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (2)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    val container3 = createContainer("host4")
    handler.handleAllocatedContainers(Array(container1, container2, container3))

    handler.getNumExecutorsRunning should be (2)
    handler.allocatedContainerToHostMap.get(container1.getId).get should be ("host1")
    handler.allocatedContainerToHostMap.get(container2.getId).get should be ("host2")
    handler.allocatedContainerToHostMap.contains(container3.getId) should be (false)
    handler.allocatedHostToContainersMap.get("host1").get should contain (container1.getId)
    handler.allocatedHostToContainersMap.get("host2").get should contain (container2.getId)
    handler.allocatedHostToContainersMap.contains("host4") should be (false)
  }

  test("decrease total requested executors") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    handler.requestTotalExecutorsWithPreferredLocalities(3, 0, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getPendingAllocate.size should be (3)

    val container = createContainer("host1")
    handler.handleAllocatedContainers(Array(container))

    handler.getNumExecutorsRunning should be (1)
    handler.allocatedContainerToHostMap.get(container.getId).get should be ("host1")
    handler.allocatedHostToContainersMap.get("host1").get should contain (container.getId)

    handler.requestTotalExecutorsWithPreferredLocalities(2, 0, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getPendingAllocate.size should be (1)
  }

  test("decrease total requested executors to less than currently running") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    handler.requestTotalExecutorsWithPreferredLocalities(3, 0, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getPendingAllocate.size should be (3)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    handler.getNumExecutorsRunning should be (2)

    handler.requestTotalExecutorsWithPreferredLocalities(1, 0, Map.empty, Set.empty)
    handler.updateResourceRequests()
    handler.getPendingAllocate.size should be (0)
    handler.getNumExecutorsRunning should be (2)
  }

  test("kill executors") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    handler.requestTotalExecutorsWithPreferredLocalities(1, 0, Map.empty, Set.empty)
    handler.executorIdToContainer.keys.foreach { id => handler.killExecutor(id ) }

    val statuses = Seq(container1, container2).map { c =>
      ContainerStatus.newInstance(c.getId(), ContainerState.COMPLETE, "Finished", 0)
    }
    handler.updateResourceRequests()
    handler.processCompletedContainers(statuses.toSeq)
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (1)
  }

  test("lost executor removed from backend") {
    val handler = createAllocator(4)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    val container1 = createContainer("host1")
    val container2 = createContainer("host2")
    handler.handleAllocatedContainers(Array(container1, container2))

    handler.requestTotalExecutorsWithPreferredLocalities(2, 0, Map(), Set.empty)

    val statuses = Seq(container1, container2).map { c =>
      ContainerStatus.newInstance(c.getId(), ContainerState.COMPLETE, "Failed", -1)
    }
    handler.updateResourceRequests()
    handler.processCompletedContainers(statuses.toSeq)
    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (2)
    handler.getNumExecutorsFailed should be (2)
    handler.getNumUnexpectedContainerRelease should be (2)
  }

  test("blacklisted nodes reflected in amClient requests") {
    // Internally we track the set of blacklisted nodes, but yarn wants us to send *changes*
    // to the blacklist.  This makes sure we are sending the right updates.
    val mockAmClient = mock(classOf[AMRMClient[ContainerRequest]])
    val handler = createAllocator(4, mockAmClient)
    handler.requestTotalExecutorsWithPreferredLocalities(1, 0, Map(), Set("hostA"))
    verify(mockAmClient).updateBlacklist(Seq("hostA").asJava, Seq[String]().asJava)

    handler.requestTotalExecutorsWithPreferredLocalities(2, 0, Map(), Set("hostA", "hostB"))
    verify(mockAmClient).updateBlacklist(Seq("hostB").asJava, Seq[String]().asJava)

    handler.requestTotalExecutorsWithPreferredLocalities(3, 0, Map(), Set())
    verify(mockAmClient).updateBlacklist(Seq[String]().asJava, Seq("hostA", "hostB").asJava)
  }

  test("memory exceeded diagnostic regexes") {
    val diagnostics =
      "Container [pid=12465,containerID=container_1412887393566_0003_01_000002] is running " +
        "beyond physical memory limits. Current usage: 2.1 MB of 2 GB physical memory used; " +
        "5.8 GB of 4.2 GB virtual memory used. Killing container."
    val vmemMsg = memLimitExceededLogMessage(diagnostics, VMEM_EXCEEDED_PATTERN)
    val pmemMsg = memLimitExceededLogMessage(diagnostics, PMEM_EXCEEDED_PATTERN)
    assert(vmemMsg.contains("5.8 GB of 4.2 GB virtual memory used."))
    assert(pmemMsg.contains("2.1 MB of 2 GB physical memory used."))
  }

  test("window based failure executor counting") {
    sparkConf.set("spark.yarn.executor.failuresValidityInterval", "100s")
    val handler = createAllocator(4)
    val clock = new ManualClock(0L)
    handler.setClock(clock)

    handler.updateResourceRequests()
    handler.getNumExecutorsRunning should be (0)
    handler.getPendingAllocate.size should be (4)

    val containers = Seq(
      createContainer("host1"),
      createContainer("host2"),
      createContainer("host3"),
      createContainer("host4")
    )
    handler.handleAllocatedContainers(containers)

    val failedStatuses = containers.map { c =>
      ContainerStatus.newInstance(c.getId, ContainerState.COMPLETE, "Failed", -1)
    }

    handler.getNumExecutorsFailed should be (0)

    clock.advance(100 * 1000L)
    handler.processCompletedContainers(failedStatuses.slice(0, 1))
    handler.getNumExecutorsFailed should be (1)

    clock.advance(101 * 1000L)
    handler.getNumExecutorsFailed should be (0)

    handler.processCompletedContainers(failedStatuses.slice(1, 3))
    handler.getNumExecutorsFailed should be (2)

    clock.advance(50 * 1000L)
    handler.processCompletedContainers(failedStatuses.slice(3, 4))
    handler.getNumExecutorsFailed should be (3)

    clock.advance(51 * 1000L)
    handler.getNumExecutorsFailed should be (1)

    clock.advance(50 * 1000L)
    handler.getNumExecutorsFailed should be (0)
  }

  private object PreemptionAllocator {
    class TestPreemptionContract extends PreemptionContract {
      private var containers = Set[PreemptionContainer]()
      private var resourceRequests = List[PreemptionResourceRequest]()

      override def getContainers: JSet[PreemptionContainer] = containers.asJava
      override def getResourceRequest: JList[PreemptionResourceRequest] = resourceRequests.asJava

      override def setContainers(containers: JSet[PreemptionContainer]): Unit =
        this.containers = containers.asScala.toSet

      override def setResourceRequest(req: JList[PreemptionResourceRequest]): Unit =
        resourceRequests = req.asScala.toList
    }

    private class TestStrictPreemptionContract extends StrictPreemptionContract {
      private var containers = Set[PreemptionContainer]()
      override def getContainers: JSet[PreemptionContainer] = containers.asJava
      override def setContainers(containers: JSet[PreemptionContainer]): Unit =
        this.containers = containers.asScala.toSet
    }

    val numContainers = 4
    val containerNames: Seq[String] = (1 to numContainers).map { i => s"host$i" }

    val handler: YarnAllocator = createAllocator(numContainers)
    handler.updateResourceRequests()

    val containers: Seq[Container] = containerNames.map(createContainer)
    handler.handleAllocatedContainers(containers)
    handler.requestTotalExecutorsWithPreferredLocalities(numContainers, 0, Map.empty, Set.empty)

    private val priority = Priority.newInstance(1)
    private val resource = Resource.newInstance(2048, 1)

    private def getExecutorIds(containerIds: Seq[ContainerId]): Set[String] = {
      handler.executorIdToContainer
        .filter { case (_, v) => containerIds.contains(v.getId) }
        .keys
        .toSet
    }

    def mkPreemptionResourceRequest(name: String): PreemptionResourceRequest = {
      PreemptionResourceRequest.newInstance(
        ResourceRequest.newInstance(priority, name, resource, 1))
    }

    def askedPreemptionTest(indexes: Seq[Int]): Unit = {
      val containerIds = indexes.map { i => containers(i).getId }

      val contract = new TestPreemptionContract
      contract.setContainers(containerIds.map(PreemptionContainer.newInstance).toSet.asJava)
      contract.setResourceRequest(
        containerNames.map(mkPreemptionResourceRequest).take(indexes.size).asJava)

      val executorIds = getExecutorIds(containerIds)
      executorIds should have size indexes.size

      val (asked, numRequested) = handler.getAskedPreemptedExecutors(contract)
      asked should have size indexes.size
      asked shouldBe executorIds
      numRequested shouldBe indexes.size
    }

    def forcedPreemptionTest(indexes: Seq[Int]): Unit = {
      val containerIds = indexes.map { i => containers(i).getId }

      val contract = new TestStrictPreemptionContract
      contract.setContainers(containerIds.map(PreemptionContainer.newInstance).toSet.asJava)

      val executorIds = getExecutorIds(containerIds)
      executorIds should have size indexes.size

      val forced = handler.getForcefullyPreemptedExecutors(contract)
      forced should have size indexes.size
      forced shouldBe executorIds
    }
  }

  import PreemptionAllocator.{askedPreemptionTest, forcedPreemptionTest}

  test("getAskedPreemptedExecutors should work even when uninitialized") {
    val numContainers = 4
    val handler = createAllocator(numContainers)
    handler.updateResourceRequests()

    val contract = new PreemptionAllocator.TestPreemptionContract
    val (asked, numRequested) = handler.getAskedPreemptedExecutors(contract)
    asked should have size 0
    numRequested shouldBe 0
  }

  test("getAskedPreemptedExecutors should select a single container") {
    Seq(0, 1, 2, 3).foreach { idx => askedPreemptionTest(Seq(idx)) }
  }

  test("getAskedPreemptedExecutors should select two containers") {
    Seq(0, 1, 2, 3).permutations.map(_.take(2)).toSet.foreach { indexes: Seq[Int] =>
      askedPreemptionTest(indexes)
    }
  }

  test("getAskedPreemptedExecutors should select three containers") {
    Seq(0, 1, 2, 3).permutations.map(_.take(3)).toSet.foreach { indexes: Seq[Int] =>
      askedPreemptionTest(indexes)
    }
  }

  test("getAskedPreemptedExecutors should select four containers") {
    Seq(0, 1, 2, 3).permutations.foreach(askedPreemptionTest)
  }

  test("getAskedPreemptedExecutors should work with invalid containers") {
    import PreemptionAllocator._
    val contract = new TestPreemptionContract
    val container = createContainer("host5")
    val preemptionContainer = PreemptionContainer.newInstance(container.getId)
    contract.setContainers(Set(preemptionContainer).asJava)
    contract.setResourceRequest(Seq(mkPreemptionResourceRequest("host5")).asJava)
    val (asked, numRequested) = handler.getAskedPreemptedExecutors(contract)
    asked should have size 0
    numRequested shouldBe 1
  }

  test("getForcefullyPreemptedExecutors should work with a single container") {
    Seq(0, 1, 2, 3).foreach { idx => forcedPreemptionTest(Seq(idx)) }
  }

  test("getForcefullyPreemptedExecutors should select two containers") {
    Seq(0, 1, 2, 3).permutations.map(_.take(2)).toSet.foreach { indexes: Seq[Int] =>
      forcedPreemptionTest(indexes)
    }
  }

  test("getForcefullyPreemptedExecutors should select three containers") {
    Seq(0, 1, 2, 3).permutations.map(_.take(3)).toSet.foreach { indexes: Seq[Int] =>
      forcedPreemptionTest(indexes)
    }
  }

  test("getForcefullyPreemptedExecutors should select four containers") {
    Seq(0, 1, 2, 3).permutations.foreach(forcedPreemptionTest)
  }
}
