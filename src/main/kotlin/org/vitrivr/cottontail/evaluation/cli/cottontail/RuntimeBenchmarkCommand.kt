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
 * @version 1.0.1
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
        private val ENTITIES = listOf("yandex_deep5m", "yandex_deep10m", "yandex_deep100m", "yandex_deep1b")

        /** List of query workloads that are being executed. */
        private val QUERIES =  arrayOf("NNS", "NNS + Fetch", "Hybrid")
    }

    /** Flag that can be used to directly provide confirmation. */
    private val k: Int by option("-k", "--k", help = "If set, then only the output will be plot.").int().default(1000)

    /** A [SplittableRandom] to generate categories. */
    private val random = SplittableRandom()

    /** Data frame that holds the measurements. */
    private val measurements: MutableMap<String,List<*>> = mutableMapOf()

    /** Data frame that holds the data. */
    private val data: MutableMap<String,List<*>> = mutableMapOf()

    /** Data frame that holds the query plans. */
    private val plans: MutableMap<String,List<*>> = mutableMapOf()

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
        this.measurements.clear()
        this.measurements[ENTITY_KEY] = mutableListOf<String>()
        this.measurements[RUN_KEY] = mutableListOf<Int>()
        this.measurements[K_KEY] = mutableListOf<Int>()
        this.measurements[INDEX_KEY] = mutableListOf<String>()
        this.measurements[PARALLEL_KEY] =  mutableListOf<Int>()
        this.measurements[RUNTIME_KEY] = mutableListOf<Double>()
        this.measurements[DCG_KEY] = mutableListOf<Double>()
        this.measurements[RECALL_KEY] = mutableListOf<Double>()
        this.measurements[QUERY_KEY] = mutableListOf<String>()

        /* Clear data map. */
        this.data.clear()
        this.data[ENTITY_KEY] = mutableListOf<String>()
        this.data[INDEX_KEY] = mutableListOf<String>()
        this.data[PARALLEL_KEY] =  mutableListOf<Int>()
        this.data[RUN_KEY] = mutableListOf<Int>()
        this.data[K_KEY] = mutableListOf<Int>()
        this.data[RESULTS_KEY] = mutableListOf<List<String>>()
        this.data[GROUNDTRUTH_KEY] = mutableListOf<List<String>>()

        /* Clear plans map. */
        this.plans.clear()
        this.plans[ENTITY_KEY] = mutableListOf<String>()
        this.plans[INDEX_KEY] = mutableListOf<String>()
        this.plans[QUERY_KEY] = mutableListOf<String>()
        this.plans[PLAN_KEY] =  mutableListOf<List<String>>()

        try {
            /* Initialise progress bar. */
            this.progress = ProgressBarBuilder()
                .setInitialMax(((this.warmup + this.repeat) * ENTITIES.size * 4).toLong())
                .setStyle(ProgressBarStyle.ASCII).setTaskName("ANNS Benchmark:").build()

            /* Execute workload. */
            for (e in ENTITIES) {
                this.runYandexDeep1B(e, k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32)
                this.runYandexDeep1B(e, k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32, indexType = "VAF")
                this.runYandexDeep1B(e, k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32, indexType = "PQ")
                this.runYandexDeep1B(e, k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32, indexType = "IVFPQ")
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
        Files.newBufferedWriter(out.resolve("measurements.json"), StandardOpenOption.CREATE_NEW).use {
            val gson = GsonBuilder().setPrettyPrinting().create()
            gson.toJson(this.measurements, Map::class.java, gson.newJsonWriter(it))
        }

        /* Export JSON data. */
        Files.newBufferedWriter(out.resolve("data.json"), StandardOpenOption.CREATE_NEW).use {
            val gson = GsonBuilder().setPrettyPrinting().create()
            gson.toJson(this.data, Map::class.java, gson.newJsonWriter(it))
        }

        /* Export JSON data. */
        Files.newBufferedWriter(out.resolve("plans.json"), StandardOpenOption.CREATE_NEW).use {
            val gson = GsonBuilder().setPrettyPrinting().create()
            gson.toJson(this.plans, Map::class.java, gson.newJsonWriter(it))
        }
    }

    /**
     * Exports a data set.
     */
    private fun runYandexDeep1B(entity: String, k: Int, warmup: Int, iterations: Int, parallel: Int, indexType: String? = null) {
        val indexName = indexType ?: "SCAN"
        YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use {
            /* Select random category for hybrid query. */
            val category = this.random.nextInt(0, 10)

            /* Warmup query. */
            this.progress?.extraMessage = "$entity (p=$parallel, index=${indexName})"
            for (w in 0 until warmup) {
                val (_, feature) = it.next()
                this.executeNNSQuery(entity, feature, k, parallel, indexType)
                this.executeNNSQueryWithFeature(entity, feature, k, parallel, indexType)
                this.executeHybridQuery(entity, feature, category, k, parallel, indexType)

                this.progress?.step()
            }

            /* Benchmark query. */
            for (r in 0 until iterations) {
                val (_, feature) = it.next()

                /* Obtain query plans. */
                if (r == 0) {
                    val qp = arrayOf(
                        explainNNSQuery(entity, feature, k, parallel, indexType),
                        explainNNSQueryWithFeature(entity, feature, k, parallel, indexType),
                        explainHybridQuery(entity, feature, category, k, parallel, indexType)
                    )
                    for ((q,p) in QUERIES.zip(qp)) {
                        (this.plans[ENTITY_KEY] as MutableList<String>) += entity
                        (this.plans[INDEX_KEY] as MutableList<String>) += indexType ?: "SCAN"
                        (this.plans[QUERY_KEY] as MutableList<String>) += q
                        (this.plans[PLAN_KEY] as MutableList<List<String>>) += p
                    }
                }

                /* Execute actual workload. */
                val results = this.executeWorkload(entity, feature, category, k, parallel, indexType)

                /* Obtain groundtrut. */
                val gt = arrayOf(
                    executeNNSQuery(entity, feature, k, 8),
                    executeNNSQueryWithFeature(entity, feature, k, 8),
                    executeHybridQuery(entity, feature, category, k, 8)
                )

                for (i in 0 until 3) {
                    /* Record data and groundtruth. */
                    (this.data[ENTITY_KEY] as MutableList<String>) += entity
                    (this.data[INDEX_KEY] as MutableList<String>) += indexType ?: "SCAN"
                    (this.data[PARALLEL_KEY] as MutableList<Int>) += parallel
                    (this.data[K_KEY] as MutableList<Int>).add(k)
                    (this.data[RUN_KEY] as MutableList<Int>) += (r + 1)
                    (this.data[GROUNDTRUTH_KEY] as MutableList<List<Int>>).add(gt[i])
                    (this.data[RESULTS_KEY] as MutableList<List<Int>>).add(results[i].second)

                    /* Record measurements. */
                    (this.measurements[ENTITY_KEY] as MutableList<String>).add(entity)
                    (this.measurements[RUN_KEY] as MutableList<Int>).add(r + 1)
                    (this.measurements[K_KEY] as MutableList<Int>).add(k)
                    (this.measurements[PARALLEL_KEY] as MutableList<Int>).add(parallel)
                    (this.measurements[INDEX_KEY] as MutableList<String>).add(indexName)
                    (this.measurements[RUNTIME_KEY] as MutableList<Double>).add(results[i].first)
                    (this.measurements[DCG_KEY] as MutableList<Double>).add(Measures.ndcg(gt[i], results[i].second))
                    (this.measurements[RECALL_KEY] as MutableList<Double>).add(Measures.recall(gt[i], results[i].second))
                    (this.measurements[QUERY_KEY] as MutableList<String>).add(QUERIES[i])
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
