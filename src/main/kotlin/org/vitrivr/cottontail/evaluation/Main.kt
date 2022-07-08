package org.vitrivr.cottontail.evaluation

import io.grpc.ManagedChannelBuilder
import jetbrains.letsPlot.elementText
import jetbrains.letsPlot.theme
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.evaluation.datasets.prepare
import org.vitrivr.cottontail.evaluation.runtime.runtime
import java.nio.file.Paths

/**
 * Host and port of Cottontail DB instance that should be tested. Please adjust.
 */
private val host = "10.34.58.81"
private val port = 1865

/** The path to the working directory. */
private val workingdir_str = "/Users/rgasser/Downloads"

/** The [ManagedChannel] to used to access Cottontail DB. */
private val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()

/** The simple client used by Cottontail DB. */
val client = SimpleClient(channel)

/** The simple client used by Cottontail DB. */
val workingdir = Paths.get(workingdir_str)


/** */
val plotTheme = theme()


/**
 *
 */
fun main(args: Array<String>) {
    when (val command = args[0]) {
        "prepare" -> prepare()
        "runtime" -> runtime()
        else -> {
            println("Unknown command '${command}'.")
        }
    }
}