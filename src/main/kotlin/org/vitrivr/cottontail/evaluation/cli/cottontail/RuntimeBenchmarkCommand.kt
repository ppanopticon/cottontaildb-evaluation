package org.vitrivr.cottontail.evaluation.cli.cottontail

import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.google.gson.GsonBuilder
import me.tongfei.progressbar.ProgressBar
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.basics.predicate.Expression
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.evaluation.datasets.YandexDeep1BIterator
import org.vitrivr.cottontail.evaluation.utilities.Measures
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import kotlin.collections.ArrayList
import kotlin.system.measureTimeMillis

/**
 * Performs a simple runtime measurement for a series of queries.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RuntimeBenchmarkCommand(private val client: SimpleClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "runtime", help = "Prepares and loads all data required for Cottontail DB benchmarks.")  {

    companion object {
        private const val PLAN_KEY = "plan"
        private const val QUERY_KEY = "query"
        private const val ENTITY_KEY = "entity"
        private const val RUN_KEY = "run"
        private const val K_KEY = "k"
        private const val RESULTS_KEY = "results"
        private const val GROUNDTRUTH_KEY = "groundtruth"
        private const val PARALLEL_KEY = "parallel"
        private const val INDEX_KEY = "index"
        private const val RUNTIME_KEY = "runtime"
        private const val DCG_KEY = "dcg"
        private const val RECALL_KEY = "recall"

        /** List of entities that should be queried. */
        private val ENTITIES = listOf("yandex_deep5m", "yandex_deep10m", "yandex_deep100m", "yandex_deep100m")
    }

    /** Flag that can be used to directly provide confirmation. */
    private val k: Int by option("-k", "--k", help = "If set, then only the output will be plot.").int().default(1000)

    /** A [SplittableRandom] to generate categories. */
    private val random = SplittableRandom()

    /** Data frame that holds the data. */
    private var data: MutableMap<String,List<*>> = mutableMapOf()

    /** Progress bar used*/
    private var progress: ProgressBar? = null

    /**
     * Executes the command.
     */
    override fun run() {
        /* Execute benchmark unless plot flag has been set. */
        val out = this.name?.let { this.workingDirectory.resolve("out/${it}") } ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
        if (!Files.exists(out)) {
            Files.createDirectories(out)
        }

        /* Clear local data. */
        this.data.clear()
        this.data[PLAN_KEY] = mutableListOf<List<String>>()
        this.data[ENTITY_KEY] = mutableListOf<String>()
        this.data[RUN_KEY] = mutableListOf<Int>()
        this.data[K_KEY] = mutableListOf<Int>()
        this.data[INDEX_KEY] = mutableListOf<String>()
        this.data[PARALLEL_KEY] =  mutableListOf<Int>()
        this.data[RUNTIME_KEY] = mutableListOf<Double>()
        this.data[DCG_KEY] = mutableListOf<Double>()
        this.data[RECALL_KEY] = mutableListOf<Double>()
        this.data[QUERY_KEY] = mutableListOf<String>()
        this.data[RESULTS_KEY] = mutableListOf<List<Int>>()
        this.data[GROUNDTRUTH_KEY] = mutableListOf<List<Int>>()

        try {
            /* Initialise progress bar. */
            this.progress = ProgressBarBuilder()
                .setInitialMax(((this.warmup + this.repeat) * ENTITIES.size * 15).toLong())
                .setStyle(ProgressBarStyle.ASCII).setTaskName("Running ANNS Benchmark...").build()

            /* Execute workload. */
            for (e in ENTITIES) {
                for (p in listOf(2, 4, 8, 16, 32)) {
                    this.runYandexDeep1B(e, k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = p)
                    this.runYandexDeep1B(e, k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = p, indexType = "BTREE")
                    this.runYandexDeep1B(e, k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = p, indexType = "PQ")
                }
            }
        } finally {
            this.export(out)

            /* Close progress bar and set to NULL. */
            this.progress?.close()
            this.progress = null
        }
    }

    /**
     * Exports a data set.
     */
    override fun export(out: Path) {
        /* Export JSON data. */
        Files.newBufferedWriter(out.resolve("data.json"), StandardOpenOption.CREATE_NEW).use {
            val gson = GsonBuilder().setPrettyPrinting().create()
            gson.toJson(this.data, Map::class.java, gson.newJsonWriter(it))
        }
    }

    /**
     * Exports a data set.
     */
    private fun runYandexDeep1B(entity: String, k: Int = 10, warmup: Int = 1, iterations: Int = 10, parallel: Int = 1, indexType: String? = null) {
        val indexName = indexType ?: "SCAN"
        YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use {
            /* Select random category for hybrid query. */
            val category = this.random.nextInt(0, 10)

            /* Warmup query. */
            this.progress?.extraMessage = "Warmup (p ?= $parallel, index = ${indexName})..."
            for (w in 0 until warmup) {
                val (_, feature) = it.next()
                this.executeNNSQuery(entity, feature, k, parallel, indexType)
                this.executeNNSQueryWithFeature(entity, feature, k, parallel, indexType)
                this.executeHybridQuery(entity, feature, category, k, parallel, indexType)

                this.progress?.step()
            }

            // Benchmark query.
            this.progress?.extraMessage = "Benchmark (p ?= $parallel, index = ${indexName})..."
            for (r in 0 until iterations) {
                val (_, feature) = it.next()
                val results = this.executeWorkload(entity, feature, category, k, parallel)

                val qp = arrayOf(
                    explainNNSQuery(entity, feature, k, 1, null),
                    explainNNSQueryWithFeature(entity, feature, k, 1, null),
                    explainHybridQuery(entity, feature, category, k, 1, "BTREE")
                )

                val gt = arrayOf(
                    executeNNSQuery(entity, feature, k, 1, null),
                    executeNNSQueryWithFeature(entity, feature, k, 1, null),
                    executeHybridQuery(entity, feature, category, k, 1, "BTREE")
                )

                /* Record data. */
                for (i in 0 until 3) {
                    (this.data[ENTITY_KEY] as MutableList<String>).add(entity)
                    (this.data[RUN_KEY] as MutableList<Int>).add(r + 1)
                    (this.data[K_KEY] as MutableList<Int>).add(k)
                    (this.data[PARALLEL_KEY] as MutableList<Int>).add(parallel)
                    (this.data[INDEX_KEY] as MutableList<String>).add(indexName)
                    (this.data[RESULTS_KEY] as MutableList<List<Int>>).add(results[i].second)
                    (this.data[GROUNDTRUTH_KEY] as MutableList<List<Int>>).add(gt[i])
                    (this.data[RUNTIME_KEY] as MutableList<Double>).add(results[i].first)
                    (this.data[DCG_KEY] as MutableList<Double>).add(Measures.ndcg(gt[i], results[i].second))
                    (this.data[RECALL_KEY] as MutableList<Double>).add(Measures.recall(gt[i], results[i].second))
                    when (i) {
                        0 -> {
                            (this.data[PLAN_KEY] as MutableList<List<String>>).add(qp[i])
                            (this.data[QUERY_KEY] as MutableList<String>).add("NNS")
                        }
                        1 -> {
                            (this.data[PLAN_KEY] as MutableList<List<String>>).add(qp[i])
                            (this.data[QUERY_KEY] as MutableList<String>).add("NNS + Fetch")
                        }
                        2 -> {
                            (this.data[PLAN_KEY] as MutableList<List<String>>).add(qp[i])
                            (this.data[QUERY_KEY] as MutableList<String>).add("Hybrid")
                        }
                        else -> throw IllegalStateException("This should not happen!")
                    }
                }

                /* Indicate progress. */
                this.progress?.step()
            }
        }
    }


    /**
     * Executes a workload. This is the equivalent of executing a single round of queries.
     *
     * @param entity Name of the entity to search.
     * @param feature The query vector [FloatArray].
     */
    private fun executeWorkload(entity: String, feature: FloatArray, category: Int, k: Int, parallel: Int, indexType: String? = null): List<Pair<Double, List<Int>>> {

        val result1: List<Int>
        val time1 = measureTimeMillis {
            result1 = this.executeNNSQuery(entity, feature, k, parallel, indexType)
        }

        val result2: List<Int>
        val time2 = measureTimeMillis {
            result2 = this.executeNNSQueryWithFeature(entity, feature, k, parallel, indexType)
        }

        val result3: List<Int>
        val time3 = measureTimeMillis {
            result3 = this.executeHybridQuery(entity, feature, category, k, parallel, indexType)
        }

        return listOf(Pair(time1 / 1000.0, result1), Pair(time2 / 1000.0, result2), Pair(time3 / 1000.0, result3))
    }

    /**
     * Executes a simple NNS query.
     */
    private fun executeNNSQuery(entity: String, feature: FloatArray, k: Int, parallel: Int, indexType: String? = null): List<Int> {
        var query = Query("evaluation.${entity}")
            .select("id")
            .distance("feature", feature, Distances.L2, "distance")
            .order("distance", Direction.ASC)
            .limit(k.toLong())

        /* Parametrise. */
        query = query.limitParallelism(parallel)
        if (indexType == null) {
            query.disallowIndex()
        } else {
            query.useIndexType(indexType)
        }

        val results = ArrayList<Int>(k)
        this.client.query(query).forEach { t ->
            results.add(t.asInt("id")!!)
        }
        return results
    }

    /**
     * Executes a simple NNS query.
     */
    private fun explainNNSQuery(entity: String, feature: FloatArray, k: Int, parallel: Int, indexType: String? = null): List<String> {
        var query = Query("evaluation.${entity}")
            .select("id")
            .distance("feature", feature, Distances.L2, "distance")
            .order("distance", Direction.ASC)
            .limit(k.toLong())

        /* Parametrise. */
        query = query.limitParallelism(parallel)
        if (indexType == null) {
            query.disallowIndex()
        } else {
            query.useIndexType(indexType)
        }

        val results = ArrayList<String>(k)
        this.client.explain(query).forEach { t ->
            results.add(t.asString("comment")!!)
        }
        return results
    }

    /**
     * Executes an NNS query that als fetches the feature vector.
     */
    private fun executeNNSQueryWithFeature(entity: String, feature: FloatArray, k: Int, parallel: Int, indexType: String? = null): List<Int> {
        var query = Query("evaluation.${entity}")
            .select("id").select("feature")
            .distance("feature", feature, Distances.L2, "distance")
            .order("distance", Direction.ASC)
            .limit(k.toLong())

        /* Parametrise. */
        query = query.limitParallelism(parallel)
        if (indexType == null) {
            query.disallowIndex()
        } else {
            query.useIndexType(indexType)
        }

        val results = ArrayList<Int>(k)
        this.client.query(query).forEach { t ->
            results.add(t.asInt("id")!!)
        }
        return results
    }

    /**
     * Executes an NNS query that als fetches the feature vector.
     */
    private fun explainNNSQueryWithFeature(entity: String, feature: FloatArray, k: Int, parallel: Int, indexType: String? = null): List<String> {
        var query = Query("evaluation.${entity}")
            .select("id").select("feature")
            .distance("feature", feature, Distances.L2, "distance")
            .order("distance", Direction.ASC)
            .limit(k.toLong())

        /* Parametrise. */
        query = query.limitParallelism(parallel)
        if (indexType == null) {
            query.disallowIndex()
        } else {
            query.useIndexType(indexType)
        }

        val results = ArrayList<String>(k)
        this.client.explain(query).forEach { t ->
            results.add(t.asString("comment")!!)
        }
        return results
    }

    /**
     * Executes an NNS query that applies a Boolean filter first.
     */
    private fun executeHybridQuery(entity: String, feature: FloatArray, category: Int, k: Int, parallel: Int, indexType: String? = null): List<Int> {
        var query = Query("evaluation.${entity}")
            .select("id")
            .distance("feature", feature, Distances.L2, "distance")
            .where(Expression("category", "=", category))
            .order("distance", Direction.ASC)
            .limit(k.toLong())

        /* Parametrise. */
        query = query.limitParallelism(parallel)
        if (indexType == null) {
            query.disallowIndex()
        } else {
            query.useIndexType(indexType)
        }

        val results = ArrayList<Int>(k)
        this.client.query(query).forEach { t ->
            results.add(t.asInt("id")!!)
        }
        return results
    }

    /**
     * Executes an NNS query that applies a Boolean filter first.
     */
    private fun explainHybridQuery(entity: String, feature: FloatArray, category: Int, k: Int, parallel: Int, indexType: String? = null): List<String> {
        var query = Query("evaluation.${entity}")
            .select("id")
            .distance("feature", feature, Distances.L2, "distance")
            .where(Expression("category", "=", category))
            .order("distance", Direction.ASC)
            .limit(k.toLong())

        /* Parametrise. */
        query = query.limitParallelism(parallel)
        if (indexType == null) {
            query.disallowIndex()
        } else {
            query.useIndexType(indexType)
        }

        val results = ArrayList<String>(k)
        this.client.explain(query).forEach { t ->
            results.add(t.asString("comment")!!)
        }
        return results
    }
}
