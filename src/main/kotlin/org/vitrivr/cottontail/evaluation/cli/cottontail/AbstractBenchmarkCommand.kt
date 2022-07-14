package org.vitrivr.cottontail.evaluation.cli.cottontail

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import org.vitrivr.cottontail.client.SimpleClient
import java.nio.file.Path
import java.nio.file.Paths

/**
 * An abstract command that represents a benchmark workload.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractBenchmarkCommand(protected val client: SimpleClient, protected val workingDirectory: Path, name: String, help: String) : CliktCommand(name = name, help = help) {

    /** The [Path] to the output folder. Defaults to "${workingDirectory}/out/${timestamp} */
    protected val output: Path by option("-o", "-out", help = "The path to the output directory.")
        .convert { Paths.get(it) }
        .default(this.workingDirectory.resolve("out/${System.currentTimeMillis()}"))


    /** The number of warm-up rounds to perform. */
    protected val warmup: Int by option("-w", "--warmup", help = "The number of warmup rounds before starting the benchmark.").int().default(1)

    /** The number of repetitions to perform for a single measurement rounds to perform. */
    protected val repeat: Int by option("-r", "--repeat", help = "The number of repetitions for a single benchmark.").int().default(5)

    /** Flag that can be used to directly provide confirmation. */
    protected val plotOnly: Boolean by option("-p", "--plot", help = "If set, then only the output will be plot.").flag()

    /**
     * Method used to generate export of the data.
     */
    abstract fun export()

    /**
     * Method used to generate plots of the data.
     */
    abstract fun plot()
}