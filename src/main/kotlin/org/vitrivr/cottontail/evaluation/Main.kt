package org.vitrivr.cottontail.evaluation

import org.vitrivr.cottontail.evaluation.cli.Cli
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.system.exitProcess

/**
 * Application's entry point.
 */
fun main(args: Array<String>) {

    /* Parse custom host and port argument. */
    val host = args.find { s -> s.startsWith("--host=") }?.split('=')?.get(1) ?: "127.0.0.1"
    val workingdir = args.find { s -> s.startsWith("--workingdir=") }?.split('=')?.get(1)
    if (workingdir == null) {
        println("Must specify a working directory $workingdir")
        exitProcess(1)
    }
    val path = Paths.get(workingdir)

    /* Create working directory. */
    if (!Files.exists(path)) {
        println("Failed to create working directory under $workingdir")
        exitProcess(1)
    }

    /* Start CLI. */
    Cli(host, path).loop()
}