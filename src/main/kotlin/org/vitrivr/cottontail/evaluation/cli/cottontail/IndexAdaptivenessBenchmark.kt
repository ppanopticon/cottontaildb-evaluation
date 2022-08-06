package org.vitrivr.cottontail.evaluation.cli.cottontail

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.clikt.parameters.types.int
import com.google.gson.GsonBuilder
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import me.tongfei.progressbar.ProgressBar
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.apache.commons.math3.stat.descriptive.MultivariateSummaryStatistics
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.basics.predicate.Expression
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.evaluation.datasets.YandexDeep1BIterator
import org.vitrivr.cottontail.evaluation.utilities.Measures
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.lang.Integer.max
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.SplittableRandom
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureTimeMillis
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class IndexAdaptivenessBenchmark(private val client: SimpleClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "index-adaptiveness", help = "Prepares and executes an index benchmark test.")  {

    companion object {
        const val TEST_ENTITY_NAME = "evaluation.yandex_adaptive_test"
        private const val TIME_KEY = "timestamp"
        private const val INSERTS_KEY = "insert"
        private const val DELETES_KEY = "delete"
        private const val COUNT_KEY = "count"
        private const val RUNTIME_KEY = "runtime"
        private const val DCG_KEY = "dcg"
        private const val RECALL_KEY = "recall"
    }

    /** Flag that can be used to directly provide confirmation. */
    private val size: Int by option("-s", "--size", help = "The start size of the collection.").int().default(1_000_000)

    /** Flag that can be used to directly provide confirmation. */
    private val duration: Int by option("-d", "--duration", help = "Duration of the run in seconds.").int().default(3600)

    /** Flag that can be used to directly provide confirmation. */
    private val threads: Int by option("-t", "--threads", help = "Duration of the run in seconds.").int().default(1)

    /** Flag that can be used to directly provide confirmation. */
    private val jitter: Boolean by option("-j", "--jitter", help = "Duration of the run in seconds.").flag("--no-jitter")

    /** The type of index to benchmark. */
    private val index by argument(name = "index", help = "The type of index to create.").enum<CottontailGrpc.IndexType>()

    /** Iterator for the data. */
    private var data: YandexDeep1BIterator? = null

    /** The [ProgressBar] used. */
    private var progress: ProgressBar? = null

    /** A [ArrayBlockingQueue] of [FloatArray] query vectors. */
    private var queue = ArrayBlockingQueue<FloatArray>(100)

    /** Data frame that holds the measurements. */
    private val measurements: MutableMap<String,List<*>> = mutableMapOf()

    /** The minimum vector seen thus far. */
    private val stat = MultivariateSummaryStatistics(96, false)

    /** A random number generator. */
    private val random = SplittableRandom()

    /** The maximum ID seen so far. */
    private val maxId = AtomicInteger()

    /** The maximum ID seen so far. */
    private val insertsExecuted = AtomicInteger()

    /** The maximum ID seen so far. */
    private val deletesExecuted = AtomicInteger()

    /**
     * Executes the benchmark.
     */
    override fun run() {
        /* Execute benchmark unless plot flag has been set. */
        val out = this.name?.let { this.workingDirectory.resolve("out/${it}") } ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
        if (!Files.exists(out)) {
            Files.createDirectories(out)
        }

        /* Clear local data. */
        this.measurements.clear()
        this.measurements[TIME_KEY] = mutableListOf<Long>()
        this.measurements[INSERTS_KEY] = mutableListOf<Int>()
        this.measurements[DELETES_KEY] = mutableListOf<Int>()
        this.measurements[RUNTIME_KEY] = mutableListOf<Double>()
        this.measurements[DCG_KEY] = mutableListOf<Double>()
        this.measurements[RECALL_KEY] = mutableListOf<Double>()

        try {
            /* Open dataset. */
            this.data = YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin"))

            /* Prepare data collection. */
            this.prepare()

            /* Run the benchmark. */
            val mutex = Mutex()
            var running = true
            runBlocking {
                /* Launch write jobs. */
                val jobs = (0 until this@IndexAdaptivenessBenchmark.threads).map {
                    launch {
                        withContext(Dispatchers.IO) {
                            while (running) {
                                this@IndexAdaptivenessBenchmark.insertOrDelete(mutex)
                            }
                        }
                    }
                }

                /* Run benchmark. */
                this@IndexAdaptivenessBenchmark.benchmark()
                running = false

                /* Cancel inserts. */
                jobs.forEach { it.join() }
            }
        } finally {
            this.data?.close()
            this.data = null

            this.progress?.close()
            this.progress = null

            this.export(out)
        }
    }

    /**
     * Exports the data files.
     */
    override fun export(out: Path) {
        /* Export JSON data. */
        Files.newBufferedWriter(out.resolve("measurements.json"), StandardOpenOption.CREATE_NEW).use {
            val gson = GsonBuilder().setPrettyPrinting().create()
            gson.toJson(this.measurements, Map::class.java, gson.newJsonWriter(it))
        }
    }


    /**
     * Executes a random insert OR delete operation.
     *
     * @param mutex The [Mutex] used to synchronise access to shared iterator.
     * @return True if action was executed, false if it was skipped.
     */
    private suspend fun insertOrDelete(mutex: Mutex): Boolean {
        val execute = this.random.nextBoolean()
        if (execute) {
            val doInsert = this.random.nextBoolean()
            if (doInsert) {
                val insertCount = this.random.nextInt(1, 10)
                val data = mutex.withLock {
                    (0 until insertCount).map { this.data!!.next() }.toList()
                }
                val insert = BatchInsert(TEST_ENTITY_NAME).columns("id", "feature")
                for ((id, feature) in data) {
                    insert.append(id, feature)
                    if (this@IndexAdaptivenessBenchmark.queue.isEmpty() && this.random.nextBoolean()) {
                        this@IndexAdaptivenessBenchmark.queue.offer(feature, 100, TimeUnit.MILLISECONDS)
                    }
                }
                this.client.insert(insert)
                this.insertsExecuted.addAndGet(insertCount)
            } else {
                val delete = Delete("evaluation.yandex_deep1b").where(Expression("id", "=", this.random.nextInt(1, this.maxId.get())))
                this.client.delete(delete)
                this.deletesExecuted.incrementAndGet()
            }
        }

        /* Wait between 5 and 1000ms. */
        delay(this.random.nextLong(5, 1000))
        return execute
    }


    /**
     * Runs the benchmark.
     */
    @OptIn(ExperimentalTime::class)
    private fun benchmark() {
        val progress = ProgressBarBuilder().setStyle(ProgressBarStyle.ASCII).setInitialMax(this.duration.toLong()).setTaskName("Index Adaptiveness Benchmark (Prepare):").build()
        val timer = TimeSource.Monotonic.markNow().plus(this.duration.seconds)
        do {
            val timestamp = timer.elapsedNow().absoluteValue.inWholeSeconds
            progress.stepTo(timestamp)
            val query = this.queue.poll(5, TimeUnit.SECONDS)

            if (query != null) {
                val (duration, results) = this.executeNNSQuery(query, this.index.toString())
                val (_, gt) = this.executeNNSQuery(query, null)

                /* Record measurements. */
                (this.measurements[TIME_KEY] as MutableList<Long>).add(timestamp)
                (this.measurements[INSERTS_KEY] as MutableList<Int>).add(this.insertsExecuted.get())
                (this.measurements[DELETES_KEY] as MutableList<Int>).add(this.deletesExecuted.get())
                (this.measurements[COUNT_KEY] as MutableList<Long>).add(this.count())
                (this.measurements[RUNTIME_KEY] as MutableList<Double>).add(duration / 1000.0)
                (this.measurements[DCG_KEY] as MutableList<Double>).add(Measures.ndcg(results, gt))
                (this.measurements[RECALL_KEY] as MutableList<Double>).add(Measures.recall(results, gt))
            }
        } while (timer.hasNotPassedNow())
    }


    /**
     * Executes a nearest neighbour search query.
     *
     */
    private fun count(): Long {
        val query = Query(TEST_ENTITY_NAME).select("*").count()
        return this.client.query(query).next().asLong("count")!!
    }

    /**
     * Executes a nearest neighbour search query.
     *
     * @param queryVector The query vector.
     * @param indexType The index to use.
     */
    private fun executeNNSQuery(queryVector: FloatArray, indexType: String? = null): Pair<Long,List<Int>> {
        var query = Query(TEST_ENTITY_NAME)
            .select("id")
            .distance("feature", queryVector, Distances.L2, "distance")
            .order("distance", Direction.ASC)
            .limit(1000)

        if (indexType == null) {
            query = query.disallowIndex()
        } else {
            query = query.useIndexType(indexType)
        }

        /* Retrieve results. */
        val results = ArrayList<Int>(1000)
        val time = measureTimeMillis {
            this.client.query(query).forEach { t -> results.add(t.asInt("id")!!) }
        }
        return time to results
    }

    /**
     * Prepares the empty entity and its index.
     */
    private fun prepare() {
        /** Drop existing entity (if it exists). */
        val drop = DropEntity(TEST_ENTITY_NAME)
        try {
            this.client.drop(drop)
        } catch (e: StatusRuntimeException) {
            if (e.status.code != Status.Code.NOT_FOUND) {
                throw e
            }
        }

        /** Create new entity from YANDEX Deep 1B dataset. */
        val create = CreateEntity(TEST_ENTITY_NAME)
            .column("id", Type.INTEGER, nullable = false)
            .column("feature", Type.FLOAT_VECTOR, 96, nullable = false)
        this.client.create(create)

        /* Clear statistics. */
        this.stat.clear()
        this.maxId.set(0)

        /** Load data. */
        val progress = ProgressBarBuilder().setStyle(ProgressBarStyle.ASCII).setInitialMax(this.size.toLong()).setTaskName("Index Adaptiveness Benchmark (Prepare):").build()
        var insert = BatchInsert(TEST_ENTITY_NAME).columns("id", "feature")
        var txId = this.client.begin()
        try {
            repeat(this.size) {
                val (id, vector) = this.data!!.next()
                this.maxId.updateAndGet { max(it, id) }

                /* Add to statistics. */
                this.stat.addValue(DoubleArray(vector.size) { vector[it].toDouble() })

                /* Intermediate commit every 1 mio entries. */
                if (id % 1_000_000 == 0) {
                    this.client.commit(txId)
                    txId = this.client.begin()
                }

                /* Append insert. */
                if (!insert.append(id, vector)) {
                    this.client.insert(insert.txId(txId))
                    insert = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature", "category")
                    insert.append(id, vector)
                }
                progress.step()
            }

            /* Final insert + commit */
            this.client.insert(insert.txId(txId))
            this.client.commit(txId)

            /** Create index. */
            when(this.index) {
                CottontailGrpc.IndexType.VAF -> this.client.create(CreateIndex(TEST_ENTITY_NAME, "feature", CottontailGrpc.IndexType.VAF).param("vaf.marks_per_dimension", "35"))
                CottontailGrpc.IndexType.PQ -> this.client.create(CreateIndex(TEST_ENTITY_NAME, "feature", CottontailGrpc.IndexType.PQ).param("pq.centroids", "4096").param("pq.subspaces","8"))
                CottontailGrpc.IndexType.IVFPQ -> this.client.create(CreateIndex(TEST_ENTITY_NAME, "feature", CottontailGrpc.IndexType.IVFPQ).param("ivfpq.centroids", "4096").param("ivfpq.subspaces","8").param("ivfpq.coarse_centroids","256"))
                else -> throw IllegalArgumentException("Unsupported index!")
            }

            /* Wait for index rebuilding to complete. */
            progress.extraMessage = "Waiting for index..."
            var state = "STALE"
            while (state != "CLEAN") {
                Thread.sleep(10000)
                val result = this.client.about(AboutEntity(TEST_ENTITY_NAME))
                result.forEach {
                    if ((it.asString("class") == "INDEX")) {
                        state = it.asString("info")!!
                    }
                }
            }
        } catch (e: Throwable) {
            this.client.rollback(txId)
        } finally {
            progress.close()
        }
    }
}