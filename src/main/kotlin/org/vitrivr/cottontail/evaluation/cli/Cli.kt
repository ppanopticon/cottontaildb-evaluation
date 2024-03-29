package org.vitrivr.cottontail.evaluation.cli

import com.datastax.oss.driver.api.core.CqlSession
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.subcommands
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.milvus.client.MilvusServiceClient
import io.milvus.param.ConnectParam
import org.jline.reader.EndOfFileException
import org.jline.reader.LineReaderBuilder
import org.jline.reader.UserInterruptException
import org.jline.terminal.TerminalBuilder
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.evaluation.cli.cottontail.*
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.regex.Pattern


/**
 * THE [Cli]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Cli(host: String, private val workingDirectory: Path) {

    companion object {
        /** The default prompt -- just fancification */
        private const val PROMPT = "\uD83E\uDD55"

        /** RegEx for splitting input lines. */
        private val LINE_SPLIT_REGEX: Pattern = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'")
    }

    /** The [ManagedChannel] to used to access Cottontail DB. */
    private val channel = ManagedChannelBuilder.forAddress(host, 1865).usePlaintext().build()

    /** The simple client used by Cottontail DB. */
    private val cottontail = SimpleClient(this.channel)

    /** The simple client used to communicate with Milvus. */
    private val milvus = MilvusServiceClient(ConnectParam.newBuilder().withHost(host).withPort(19530).build())

    /** The simple client used to communicate with Milvus. */
    private val cassandra = try {
        CqlSession.builder().addContactPoint(InetSocketAddress(host, 9042)).withLocalDatacenter("datacenter1").build()
    } catch (e: Throwable) {
        System.err.println("Apache Cassandra not available under ${host}:9042")
        null
    }

    /** Flag indicating whether [Cli] has been stopped. */
    @Volatile
    private var stopped: Boolean = false

    /** Generates a new instance of [CottontailCommand]. */
    private val clikt = BenchmarkCommand()


    init {
        Runtime.getRuntime().addShutdownHook(object: Thread() {
            override fun run() {
                this@Cli.channel.shutdown()
                this@Cli.milvus.close()
            }
        })
    }

    /**
     * Tries to execute the given CLI command.
     */
    fun execute(command: String) = try {
        this.clikt.parse(splitLine(command))
        println()
    } catch (e: Exception) {
        when (e) {
            is com.github.ajalt.clikt.core.PrintHelpMessage -> println(e.command.getFormattedHelp())
            is com.github.ajalt.clikt.core.NoSuchSubcommand,
            is com.github.ajalt.clikt.core.MissingArgument,
            is com.github.ajalt.clikt.core.MissingOption,
            is com.github.ajalt.clikt.core.BadParameterValue,
            is com.github.ajalt.clikt.core.NoSuchOption,
            is com.github.ajalt.clikt.core.UsageError -> println(e.localizedMessage)
            is StatusException, /* Exceptions reported by Cottontail DB via gRPC. */
            is StatusRuntimeException -> println(e.localizedMessage)
            else -> println(e.printStackTrace())
        }
    }

    /**
     * Blocking REPL of the CLI
     */
    fun loop() {
        val terminal = try {
            TerminalBuilder.builder().jna(true).build()
        } catch (e: IOException) {
            System.err.println("Could not initialize terminal: ${e.message}. Terminating...")
            return
        }

        /* Start CLI loop. */
        val lineReader = LineReaderBuilder.builder().terminal(terminal).build()
        while (!this.stopped) {
            /* Catch ^D end of file as exit method */
            val line = try {
                lineReader.readLine(PROMPT).trim()
            } catch (e: EndOfFileException) {
                System.err.println("Could not read from terminal.")
                break
            } catch (e: UserInterruptException) {
                System.err.println("Program was interrupted by the user (Ctrl-C).")
                break
            }

            if (line.lowercase() == "help") {
                println(clikt.getFormattedHelp())
                continue
            }
            if (line.isBlank()) {
                continue
            }

            /* Execute command. */
            this.execute(line)

            /* Sleep for a few milliseconds. */
            Thread.sleep(100)
        }

        /** Closes both clients. */
        this.cottontail.close()
        this.milvus.close()
    }

    /**
     * Stops the CLI loop.
     */
    fun stop() {
        this.stopped = true
    }

    //based on https://stackoverflow.com/questions/366202/regex-for-splitting-a-string-using-space-when-not-surrounded-by-single-or-double/366532
    private fun splitLine(line: String?): List<String> {
        if (line == null || line.isEmpty()) {
            return emptyList()
        }
        val matchList: MutableList<String> = ArrayList()
        val regexMatcher = LINE_SPLIT_REGEX.matcher(line)
        while (regexMatcher.find()) {
            when {
                regexMatcher.group(1) != null -> matchList.add(regexMatcher.group(1))
                regexMatcher.group(2) != null -> matchList.add(regexMatcher.group(2))
                else -> matchList.add(regexMatcher.group())
            }
        }
        return matchList
    }

    /**
     * Wrapper class and single access point to the actual commands. Also, provides the gRPC bindings.
     */
    inner class BenchmarkCommand : NoOpCliktCommand(name = "cottontail", help = "The base command for all CLI commands.") {
        init {
            subcommands(
                /* Entity related commands. */
                object : NoOpCliktCommand(
                    name = "cottontail",
                    help = "Groups commands that act on Cottontail DB.",
                    invokeWithoutSubcommand = true,
                    printHelpOnEmptyArgs = true
                ) {
                    override fun aliases(): Map<String, List<String>> = emptyMap()
                }.subcommands(
                    LoadDataCommand(this@Cli.cottontail, this@Cli.workingDirectory),
                    IndexAdaptivenessBenchmark(this@Cli.cottontail, this@Cli.workingDirectory),
                    IndexQualityBenchmarkCommand(this@Cli.cottontail, this@Cli.workingDirectory),
                    MultimediaAnalyticsBenchmark(this@Cli.cottontail, this@Cli.workingDirectory),
                    MultimediaAnalyticsNoOptBenchmark(this@Cli.cottontail, this@Cli.workingDirectory),
                    CostModelBenchmark(this@Cli.cottontail, this@Cli.workingDirectory),
                    RuntimeBenchmarkCommand(this@Cli.cottontail, this@Cli.workingDirectory),
                    MultimediaIndexBenchmark(this@Cli.cottontail, this@Cli.workingDirectory)
                ),

                /* Schema related commands. */
                object : NoOpCliktCommand(
                    name = "milvus",
                    help = "Groups commands that act on Milvus.",
                    invokeWithoutSubcommand = true,
                    printHelpOnEmptyArgs = true
                ) {
                    override fun aliases(): Map<String, List<String>> = emptyMap()
                }.subcommands(
                    org.vitrivr.cottontail.evaluation.cli.milvus.LoadDataCommand(this@Cli.milvus, this@Cli.workingDirectory),
                    org.vitrivr.cottontail.evaluation.cli.milvus.RuntimeBenchmarkCommand(this@Cli.milvus, this@Cli.workingDirectory)
                ),

                /* Schema related commands. */
                object : NoOpCliktCommand(
                    name = "cassandra",
                    help = "Groups commands that act on Apache Cassandra.",
                    invokeWithoutSubcommand = true,
                    printHelpOnEmptyArgs = true
                ) {
                    override fun aliases(): Map<String, List<String>> = emptyMap()
                }.subcommands(
                    org.vitrivr.cottontail.evaluation.cli.cassandra.RuntimeBenchmarkCommand(this@Cli.cassandra, this@Cli.workingDirectory)
                ),

                /* General commands. */
                StopCommand()
            )
        }

        /**
         * Stops the entire application
         */
        inner class StopCommand : CliktCommand(name = "stop", help = "Stops this CLI.") {
            override fun run() {
                println("Stopping application now...")
                this@Cli.stop()
            }
        }
    }
}