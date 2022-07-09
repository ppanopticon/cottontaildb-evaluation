package org.vitrivr.cottontail.evaluation

import io.grpc.ManagedChannelBuilder
import jetbrains.letsPlot.theme
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.evaluation.datasets.prepare
import org.vitrivr.cottontail.evaluation.runtime.plot
import org.vitrivr.cottontail.evaluation.runtime.runtime
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.system.exitProcess

/**
 * Host and port of Cottontail DB instance that should be tested. Please adjust.
 */
private val host = "127.0.0.1"
private val port = 1865

/** The path to the working directory. */
private val basepath = "/tank/evaluation"

/** The [ManagedChannel] to used to access Cottontail DB. */
private val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()

/** The simple client used by Cottontail DB. */
val client = SimpleClient(channel)

/** Default theme for plotting. */
val plotTheme = theme()

/** The current timestamp. */
val timestamp = System.currentTimeMillis()

/** Path to the working directory. */
val workingdir = Paths.get(basepath)

/** Path to the output directory. */
val outdir = workingdir.resolve("out/$timestamp")

/**
 *
 */
fun main(args: Array<String>) {
    /* Create working directory. */
    if (!Files.exists(workingdir)) {
        println("Failed to create working directory under $workingdir")
        exitProcess(1)
    }

    /* Evaluate command to execute. */
    when (val command = args[0]) {
        "prepare" -> prepare()
        "runtime" -> runtime()
        "plot" -> {
            val name = args[1]
            val path = Paths.get(args[2])
            plot(name, path)
        }
        else -> {
            println("Unknown command '${command}'.")
        }
    }
}