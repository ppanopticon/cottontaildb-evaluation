package org.vitrivr.cottontail.evaluation.cli.cottontail

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.clikt.parameters.types.float
import com.github.ajalt.clikt.parameters.types.int
import com.google.gson.GsonBuilder
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.*
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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.absoluteValue
import kotlin.system.measureTimeMillis
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IndexAdaptivenessBenchmark(private val client: SimpleClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "index-adaptiveness", help = "Prepares and executes an index benchmark test.")  {

    companion object {

        const val MIN_OP = 100
        const val MAX_OP = 5000

        const val TEST_ENTITY_NAME = "evaluation.yandex_adaptive_test"
        const val INDEX_NAME = "test_index"

        private const val TIME_KEY = "timestamp"
        private const val INSERTS_KEY = "insert"
        private const val DELETES_KEY = "delete"
        private const val OOB_KEY = "oob"
        private const val K_KEY = "k"
        private const val REBUILT_KEY = "rebuilt"
        private const val COUNT_KEY = "count"
        private const val RUNTIME_KEY = "runtime"
        private const val DCG_KEY = "dcg"
        private const val RECALL_KEY = "recall"
        private const val PLAN_KEY = "plan"
    }

    /** Flag that can be used to directly provide confirmation. */
    private val size: Int by option("-s", "--size", help = "The start size of the collection.").int().default(1_000_000)

    /** Flag that can be used to directly provide confirmation. */
    private val duration: Int by option("-d", "--duration", help = "Duration of the run in seconds.").int().default(3600)

    /** Flag that can be used to directly provide confirmation. */
    private val rebuildAfter: Int by option("-b", "--rebuild", help = "Duration in seconds after which index should be rebuilt.").int().default(-1)

    /** Flag that can be used to directly provide confirmation. */
    private val jitter: Int by option("-j", "--jitter", help = "Duration of the run in seconds.").int().default(0)

    /** Flag that can be used to directly provide confirmation. */
    private val inserts: Float by option("-I", "--inserts", help = "Percentage of insert operations.").float().default(0.5f)

    /** Flag that can be used to directly provide confirmation. */
    private val deletes: Float by option("-D", "--deletes", help = "Percentage of delete operations.").float().default(0.5f)

    /** The type of index to benchmark. */
    private val index by argument(name = "index", help = "The type of index to create.").enum<CottontailGrpc.IndexType>()

    /** Iterator for the data. */
    private var data: YandexDeep1BIterator? = null

    /** The [ProgressBar] used. */
    private var progress: ProgressBar? = null

    /** Data frame that holds the measurements. */
    private val measurements: MutableMap<String,List<*>> = mutableMapOf()

    /** The minimum vector seen thus far. */
    private val stat = MultivariateSummaryStatistics(96, false)

    /** A random number generator. */
    private val random = SplittableRandom()

    /** The maximum ID seen so far. */
    private val maxId = AtomicInteger()

    /** The maximum ID seen so far. */
    private val insertsExecuted = AtomicLong()

    /** The maximum ID seen so far. */
    private val deletesExecuted = AtomicLong()

    /** The number of tombstone entries expected. */
    private val tombstonesCounter = AtomicInteger()

    /** The number of tombstone entries expected. */
    private val indexRebuilt = AtomicBoolean(false)

    /** List of deleted IDs. */
    private val deleted = HashSet<Int>()

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
        this.measurements[COUNT_KEY] = mutableListOf<Long>()
        this.measurements[INSERTS_KEY] = mutableListOf<Int>()
        this.measurements[DELETES_KEY] = mutableListOf<Int>()
        this.measurements[OOB_KEY] = mutableListOf<Int>()
        this.measurements[RUNTIME_KEY] = mutableListOf<Double>()
        this.measurements[DCG_KEY] = mutableListOf<Double>()
        this.measurements[RECALL_KEY] = mutableListOf<Double>()
        this.measurements[REBUILT_KEY] = mutableListOf<Boolean>()
        this.measurements[K_KEY] = mutableListOf<Int>()
        this.measurements[PLAN_KEY] = mutableListOf<Pair<String,Float>>()

        /* Reset maximum ID and list of deleted items. */
        this.maxId.set(0)
        this.deleted.clear()

        /* Reset counters. */
        this.insertsExecuted.set(0)
        this.deletesExecuted.set(0)
        this.tombstonesCounter.set(0)
        this.indexRebuilt.set(false)

        /* Reset statistics. */
        this.stat.clear()

        try {
            /* Open dataset. */
            this.data = YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/base.1B.fbin"))

            /* Prepare data collection. */
            this.prepare()

            /* Run the benchmark. */
            runBlocking {
                val insert = launch(Dispatchers.IO) {
                    while (this.isActive) {
                        this@IndexAdaptivenessBenchmark.doInsert()
                        delay(this@IndexAdaptivenessBenchmark.random.nextLong(50, 500))
                    }
                }
                val delete = launch(Dispatchers.IO) {
                    while (this.isActive) {
                        this@IndexAdaptivenessBenchmark.doDelete()
                        delay(this@IndexAdaptivenessBenchmark.random.nextLong(50, 500))
                    }
                }

                val rebuild = if (this@IndexAdaptivenessBenchmark.rebuildAfter > 0) {
                    launch(Dispatchers.IO) {
                        delay(this@IndexAdaptivenessBenchmark.rebuildAfter.toLong())
                        this@IndexAdaptivenessBenchmark.indexRebuilt.compareAndSet(false, true)
                        this@IndexAdaptivenessBenchmark.client.rebuild(RebuildIndex("${TEST_ENTITY_NAME}.${INDEX_NAME}").async())
                    }
                } else {
                    null
                }

                /* Run benchmark. */
                this@IndexAdaptivenessBenchmark.benchmark()
                try {
                    this@IndexAdaptivenessBenchmark.export(out)
                } catch (e: Throwable) {
                    this@IndexAdaptivenessBenchmark.export(out.parent.resolve("${out.fileName}~${System.currentTimeMillis()}"))
                }

                /* Wait for jobs to finish inserts. */
                insert.cancelAndJoin()
                delete.cancelAndJoin()
                rebuild?.cancelAndJoin()
            }
        } catch (e: Throwable) {
            println("An error has occurred: ${e.message}")
        } finally{
            this.data?.close()
            this.data = null

            this.progress?.close()
            this.progress = null
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
     * Executes a INSERT operation.
     */
    private fun doInsert() {
        try {
            val insertCount = this.random.nextInt((MIN_OP * this.inserts).toInt(), (MAX_OP * this.inserts).toInt())
            if (insertCount > 0) {
                val vectors = (0 until insertCount).map {
                    val vec = this.data!!.next()
                    if (this.jitter > 0) {
                        for (i in vec.second.indices) {
                            val noise = this.stat.mean[i].absoluteValue * this.jitter
                            if (this.random.nextBoolean()) { /* Introduce noise. */
                                vec.second[i] += this.random.nextDouble(0.0, noise).toFloat()
                            } else {
                                vec.second[i] -= this.random.nextDouble(0.0, noise).toFloat()
                            }
                        }
                    }
                    vec
                }.toList()

                val insert = BatchInsert(TEST_ENTITY_NAME).columns("id", "feature")
                for ((id, feature) in vectors) {
                    insert.append(id, feature)

                    /* Check if entry lies out of bounds. */
                    for (i in feature.indices) {
                        if (feature[i] > this.stat.max[i] || feature[i] < this.stat.min[i]) {
                            this.tombstonesCounter.incrementAndGet()
                            break
                        }
                    }
                }
                while (true) {
                    try {
                        this.client.insert(insert).next()
                        break
                    } catch (e: StatusRuntimeException) {
                        if (e.status.code == Status.Code.RESOURCE_EXHAUSTED) {
                            continue /* Retry. */
                        } else {
                            throw e
                        }
                    }
                }
                this.insertsExecuted.addAndGet(insertCount.toLong())
            }
        } catch (e: Throwable) {
            System.err.println("An error occurred during insert: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Executes a DELETE operation.
     */
    private fun doDelete()  {
        try {
            val deleteCount = this.random.nextInt((MIN_OP * this.deletes).toInt(), (MAX_OP * this.deletes).toInt())
            if (deleteCount > 0) {
                val deletes = (0 until deleteCount).map {
                    var deleteId: Int
                    do {
                        deleteId = this.random.nextInt(1, this.maxId.get())
                    } while (this.deleted.contains(deleteId))
                    this.deleted.add(deleteId)
                    deleteId
                }
                val delete = Delete(TEST_ENTITY_NAME).where(Expression("id", "IN", deletes))
                var deleted: Long
                while (true) {
                    try {
                        deleted = this.client.delete(delete).next().asLong("deleted")!!
                        break
                    } catch (e: StatusRuntimeException) {
                        if (e.status.code == Status.Code.RESOURCE_EXHAUSTED) {
                            continue /* Retry upon conflict. */
                        } else {
                            throw e
                        }
                    }
                }

                this.deletesExecuted.addAndGet(deleted)
            }
        } catch (e: Throwable) {
            System.err.println("An error occurred during delete: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Runs the benchmark.
     */
    @OptIn(ExperimentalTime::class)
    private suspend fun benchmark() {
        val progress = ProgressBarBuilder().setStyle(ProgressBarStyle.ASCII).setInitialMax(this.duration.toLong()).setTaskName("Index Adaptiveness Benchmark (Prepare):").build()
        val timer = TimeSource.Monotonic.markNow().plus(this.duration.seconds)
        var queries = YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin"))
        try {
            do {
                val timestamp = this.duration - timer.elapsedNow().absoluteValue.inWholeSeconds
                progress.stepTo(timestamp)
                if (queries.hasNext()) {
                    val (id, query) = queries.next()
                    try {
                        val (duration, plan, results) = this.executeNNSQuery(query, this.index.toString())

                        /* Record measurements. */
                        (this.measurements[TIME_KEY] as MutableList<Long>).add(timestamp)
                        (this.measurements[INSERTS_KEY] as MutableList<Long>).add(this.insertsExecuted.get())
                        (this.measurements[DELETES_KEY] as MutableList<Long>).add(this.deletesExecuted.get())
                        (this.measurements[OOB_KEY] as MutableList<Int>).add(this.tombstonesCounter.get())
                        (this.measurements[COUNT_KEY] as MutableList<Long>).add(this.count())
                        (this.measurements[RUNTIME_KEY] as MutableList<Double>).add(duration / 1000.0)
                        (this.measurements[DCG_KEY] as MutableList<Double>).add(Measures.ndcg(results.first, results.second))
                        (this.measurements[RECALL_KEY] as MutableList<Double>).add(Measures.recall(results.first, results.second))
                        (this.measurements[REBUILT_KEY] as MutableList<Boolean>).add(this.indexRebuilt.get())
                        (this.measurements[K_KEY] as MutableList<Int>).add(results.first.size)
                        (this.measurements[PLAN_KEY] as MutableList<Pair<String, Float>>).add(plan.last())

                        delay(this.random.nextLong(100, 1000))
                    } catch (e: Throwable) {
                        System.err.println("An error occurred during select ${e.message} $")
                        e.printStackTrace()
                    }
                } else {
                    queries.close()
                    queries = YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin"))
                }
            } while (timer.hasNotPassedNow())
        } finally {
            queries.close()
        }
    }

    /**
     * Executes a nearest neighbour search query.
     *
     */
    private fun count(): Long {
        val query = Query(TEST_ENTITY_NAME).select("*").count()
        return this.client.query(query).next().asLong("count(*)")!!
    }

    /**
     * Executes a nearest neighbour search query.
     *
     * @param queryVector The query vector.
     * @param indexType The index to use.
     */
    private fun executeNNSQuery(queryVector: FloatArray, indexType: String): Triple<Long,List<Pair<String,Float>>,Pair<List<Int>,List<Int>>> {
        val txId = this.client.begin(true)
        try {
            val query = Query(TEST_ENTITY_NAME)
                .select("id")
                .distance("feature", queryVector, Distances.L2, "distance")
                .order("distance", Direction.ASC)
                .limit(1000)
                .txId(txId)

            /* Retrieve execution plan. */
            val plan = ArrayList<Pair<String,Float>>()
            this.client.explain(query.useIndexType(indexType)).forEach {
                if (it.asInt("rank") == 1) {
                plan.add(it.asString("designation")!! to it.asFloat("score")!!)
            }}

            /* Retrieve results. */
            val results = ArrayList<Int>(1000)
            val gt = ArrayList<Int>(1000)
            val time = measureTimeMillis {
                this.client.query(query.useIndexType(indexType)).forEach { t -> results.add(t.asInt("id")!!) }
            }
            this.client.query(query.disallowIndex()).forEach { t -> gt.add(t.asInt("id")!!) }
            return Triple(time,  plan,results to gt)
        } finally {
            this.client.rollback(txId)
        }
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
            for (i in 0 until this.size) {
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
                    insert = BatchInsert(TEST_ENTITY_NAME).columns("id", "feature")
                    insert.append(id, vector)
                }
                progress.step()
            }

            /* Final insert + commit */
            this.client.insert(insert.txId(txId))
            this.client.commit(txId)

            /** Create index. */
            when(this.index) {
                CottontailGrpc.IndexType.VAF -> this.client.create(CreateIndex(TEST_ENTITY_NAME, "feature", CottontailGrpc.IndexType.VAF).param("vaf.marks_per_dimension", "35").name(INDEX_NAME))
                CottontailGrpc.IndexType.PQ -> this.client.create(CreateIndex(TEST_ENTITY_NAME, "feature", CottontailGrpc.IndexType.PQ).param("pq.centroids", "1024").param("pq.subspaces","8").name(INDEX_NAME))
                CottontailGrpc.IndexType.IVFPQ -> this.client.create(CreateIndex(TEST_ENTITY_NAME, "feature", CottontailGrpc.IndexType.IVFPQ).param("ivfpq.centroids", "512").param("ivfpq.subspaces","8").param("ivfpq.coarse_centroids","128").name(INDEX_NAME))
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
            throw e
        } finally {
            progress.close()
        }
    }
}