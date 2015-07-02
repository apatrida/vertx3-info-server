package com.collokia.vertx.cluster

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.amazonaws.services.ec2.model.Filter
import com.amazonaws.services.ec2.model.Instance
import com.hazelcast.config.Config
import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.net.URL
import java.util.ArrayList
import java.util.concurrent.CountDownLatch

val VERTX_CLUSTER_TAG = "VertxCluster"

public data class VertxClusterWrapper(val vertx: Vertx, val clusterManager: HazelcastClusterManager)

public fun buildVertxCluster(): VertxClusterWrapper {
    val instanceId = getEc2InstanceId()
    if (instanceId != null) {
        val ec2Client = AmazonEC2Client(DefaultAWSCredentialsProviderChain())
        val instance = getEc2Instance(ec2Client, instanceId)
        if (instance != null) {
            val clusterName = getVertxClusterTag(instance)
            if (clusterName != null) {
                val clusterMembership = getClusterMembershipList(ec2Client, clusterName)
                return syncStartVertxCluster(buildClusterOptions(clusterName, instance, clusterMembership))
            }
        }
    }
    val hazelcastConfig = Config()
    hazelcastConfig.setProperty("hazelcast.initial.min.cluster.size", "2")
    val networkConfig = hazelcastConfig.getNetworkConfig()
    networkConfig.setPort(System.getProperty("HazelcastPort").toInt())
    networkConfig.setPortAutoIncrement(false)
    val clusterManager = HazelcastClusterManager(hazelcastConfig)
    return syncStartVertxCluster(VertxOptions().setClusterManager(clusterManager))
}

public fun buildClusterOptions(clusterName: String, thisInstance: Instance, clusterMembership: List<Instance>): VertxOptions {
    if (clusterMembership.isEmpty()) {
        throw IllegalStateException("Unable to configure hazelcast cluster. Cluster membership is empty.")
    }
    val hazelcastConfig = Config()
    hazelcastConfig.setProperty("hazelcast.initial.min.cluster.size", Math.max(2, (clusterMembership.size() / 2) + 1).toString())
    hazelcastConfig.getGroupConfig().setName(clusterName).setPassword(clusterName)
    val networkConfig = hazelcastConfig.getNetworkConfig()
    networkConfig.setPort(System.getProperty("HazelcastPort").toInt())
    networkConfig.setPortAutoIncrement(false)
    networkConfig.setPublicAddress(thisInstance.getPublicDnsName())
    val joinConfig = networkConfig.getJoin()
    joinConfig.getMulticastConfig().setEnabled(false)
    val tcpIpConfig = joinConfig.getTcpIpConfig().setRequiredMember(null).setEnabled(true)
    val joinAddresses = ArrayList<String>()
    clusterMembership.forEach { instance ->
        joinAddresses.add(instance.getPublicDnsName())
    }
    tcpIpConfig.setMembers(joinAddresses)
    tcpIpConfig.setConnectionTimeoutSeconds(30)
    val clusterManager = HazelcastClusterManager(hazelcastConfig)
    return VertxOptions().setClusterManager(clusterManager)
}

private fun syncStartVertxCluster(options: VertxOptions): VertxClusterWrapper {
    val latch = CountDownLatch(1)
    var tempVertx: Vertx? = null
    var error: Throwable? = null
    Vertx.clusteredVertx(options, { res: AsyncResult<Vertx> ->
        if (res.succeeded()) {
            tempVertx = res.result()
            latch.countDown()
        } else {
            error = res.cause()
            latch.countDown()
        }
    })
    while (latch.getCount() > 0) {
        latch.await()
    }
    val vertx = tempVertx
    if (vertx == null) {
        if (error == null) {
            throw IllegalStateException("Unable to initialize hazelcast cluster for unknown reason.")
        } else {
            throw IllegalStateException("Unable to initialize hazelcast cluster.", error)
        }
    } else {
        return VertxClusterWrapper(vertx, options.getClusterManager() as HazelcastClusterManager)
    }
}

public fun getClusterMembershipList(ec2Client: AmazonEC2Client, clusterName: String): List<Instance> {
    val membershipList = ArrayList<Instance>()
    try {
        ec2Client.describeInstances(DescribeInstancesRequest().withFilters(listOf(Filter("tag:${VERTX_CLUSTER_TAG}", listOf(clusterName))))).getReservations().forEach { reservation ->
            reservation.getInstances().forEach { instance ->
                if (instance != null) {
                    membershipList.add(instance)
                }
            }
        }
    } catch (e: Exception) {
        e.printStackTrace()
    }
    return membershipList
}

public fun getVertxClusterTag(instance: Instance): String? {
    instance.getTags()?.forEach { tag ->
        if (tag.getKey() == VERTX_CLUSTER_TAG) {
            return tag.getValue()
        }
    }
    return null
}

public fun getEc2InstanceId(): String? {
    try {
        val connection = URL("http://169.254.169.254/latest/meta-data/instance-id").openConnection()
        connection.setConnectTimeout(2000)
        connection.setReadTimeout(2000)
        connection.connect()
        connection.getInputStream().bufferedReader("UTF-8").use {
            return it.readText()
        }
    } catch (e: Exception) {
        e.printStackTrace()
    }
    return null
}

public fun getEc2Instance(ec2Client: AmazonEC2Client, instanceId: String): Instance? {
    try {
        ec2Client.describeInstances(DescribeInstancesRequest().withInstanceIds(listOf(instanceId))).getReservations().forEach { reservation ->
            reservation.getInstances().forEach { instance ->
                return instance
            }
        }
    } catch (e: Exception) {
        e.printStackTrace()
    }
    return null
}