package org.vitrivr.cottontail.evaluation

import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient
import java.nio.file.Paths

/**
 * Host and port of Cottontail DB instance that should be tested. Please adjust.
 */
private val host = "127.0.0.1"
private val port = 1865

/** The path to the working directory. */
private val workingdir_str = "/tank/evaluation"

/** The [ManagedChannel] to used to access Cottontail DB. */
private val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()

/** The simple client used by Cottontail DB. */
val client = SimpleClient(channel)

/** The simple client used by Cottontail DB. */
val workingdir = Paths.get(workingdir_str)

/**
 *
 */
fun main() {
}