package com.collokia.vertx.infoserver

import com.collokia.vertx.cluster.buildVertxCluster
import com.hazelcast.core.Member
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.Comparator

public fun main(args: Array<String>) {
    val vertxWrapper = buildVertxCluster()
    val clusterManager = vertxWrapper.clusterManager
    val vertx = vertxWrapper.vertx
    val server = vertx.createHttpServer()
    server.requestHandler({ request ->
        request.response().putHeader("Content-Type", "text/plain").end(buildClusterInfoText(clusterManager))
    }).listen(System.getProperty("ServicePort").toInt())
}

public fun buildClusterInfoText(clusterManager: HazelcastClusterManager): String {
    val output = StringBuilder()
    clusterManager.getHazelcastInstance().getCluster().getMembers().sortBy(object: Comparator<Member> {
        override fun compare(member1: Member, member2: Member): Int {
            val socketAddress1 = member1.getSocketAddress()
            val socketAddress2 = member2.getSocketAddress()
            val address1 = socketAddress1.getAddress()
            val address2 = socketAddress2.getAddress()
            var diff = address1.getCanonicalHostName().compareTo(address2.getCanonicalHostName())
            if (diff != 0) return diff
            diff = address1.getHostAddress().compareTo(address2.getHostAddress())
            if (diff != 0) return diff
            diff = socketAddress1.getPort().compareTo(socketAddress2.getPort())
            if (diff != 0) return diff
            return member1.getUuid().compareTo(member2.getUuid())
        }
    }).forEach { member ->
        val address = member.getSocketAddress()
        val host = address.getAddress().getCanonicalHostName()
        val ip = address.getAddress().getHostAddress()
        val port = address.getPort()
        val uuid = member.getUuid()
        if (member.localMember()) {
            output.append(" me -> ")
        } else {
            output.append("       ")
        }
        output.append(host).append(" (").append(ip).append(") : ").append(port).append(" - ").append(uuid).append("\n")
    }
    return output.toString()
}
