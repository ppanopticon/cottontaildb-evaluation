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
import org.vitrivr.cottontail.evaluation.utilities.Measures
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import kotlin.collections.ArrayList
import kotlin.system.measureTimeMillis

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class MultimediaAnalyticsBenchmark (private val client: SimpleClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "analytics", help = "Prepares and loads all data required for Cottontail DB benchmarks.")  {
    companion object {
        private const val PLAN_KEY = "plan"
        private const val QUERY_KEY = "query"
        private const val ENTITY_KEY = "entity"
        private const val RUN_KEY = "run"
        private const val K_KEY = "k"
        private const val PARALLEL_KEY = "parallel"
        private const val INDEX_KEY = "index"
        private const val RUNTIME_KEY = "runtime"
        private const val DCG_KEY = "ndcg_range"
        private const val RECALL_KEY = "recall_range"
        private const val RESULTS_KEY = "results_range"
        private const val GROUNDTRUTH_KEY = "groundtruth_range"

        /** List of entities that should be queried. */
        private val ENTITIES = listOf(
            "features_averagecolor",
            "features_visualtextcoembedding",
            "features_hogmf25k512",
            "features_inceptionresnetv2",
            "features_conceptmasksade20k"
        )

        /** List of index structures that should be used. */
        private val INDEXES = listOf(null, "PQ", "VAF")

        /** List of parallelism levels that should be tested. */
        private val PARALLEL = listOf(2, 4, 8, 16, 32)

        /** List of index structures that should be used. */
        private val QUERIES = listOf("Fetch", "Mean", "Range", "NNS", "Select")
    }

    /** Flag that can be used to directly provide confirmation. */
    private val k: Int by option("-k", "--k", help = "If set, then only the output will be plot.").int().default(1000)

    /** A [SplittableRandom] to generate categories. */
    private val random = SplittableRandom()

    /** Data frame that holds the data. */
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

        /* Clear measurements map. */
        this.measurements.clear()
        this.measurements[ENTITY_KEY] = mutableListOf<String>()
        this.measurements[QUERY_KEY] = mutableListOf<String>()
        this.measurements[INDEX_KEY] = mutableListOf<String>()
        this.measurements[PARALLEL_KEY] =  mutableListOf<Int>()
        this.measurements[K_KEY] = mutableListOf<Int>()
        this.measurements[RUN_KEY] = mutableListOf<Int>()
        this.measurements[RUNTIME_KEY] = mutableListOf<Double>()
        this.measurements[DCG_KEY] = mutableListOf<Double>()
        this.measurements[RECALL_KEY] = mutableListOf<Double>()

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
                .setInitialMax((ENTITIES.size  * INDEXES.size * PARALLEL.size * this.repeat).toLong())
                .setStyle(ProgressBarStyle.ASCII).setTaskName("Multimedia Analytics Benchmark:").build()

            /* Execute workload. */
            for (entity in ENTITIES) {
                for (index in INDEXES) {
                    for (p in PARALLEL) {
                        executeWorkload(entity, p, index)
                    }
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
        /* Export measurements. */
        Files.newBufferedWriter(out.resolve("measurements.json"), StandardOpenOption.CREATE_NEW).use {
            val gson = GsonBuilder().setPrettyPrinting().create()
            gson.toJson(this.measurements, Map::class.java, gson.newJsonWriter(it))
        }

        /* Export raw data. */
        Files.newBufferedWriter(out.resolve("data.json"), StandardOpenOption.CREATE_NEW).use {
            val gson = GsonBuilder().setPrettyPrinting().create()
            gson.toJson(this.data, Map::class.java, gson.newJsonWriter(it))
        }
    }

    /**
     * Executes a workload. This is the equivalent of executing a single round of queries.
     *
     * @param entity Name of the entity to search.
     * @param parallel The parallelism level to test.
     * @param indexType The index type to use.
     */
    private fun executeWorkload(entity: String, parallel: Int, indexType: String? = null) {
        /** */
        for (r in 0 until this.repeat) {
            this.progress!!.step()

            /* Random vector. */
            val (time1, queryVector, plan1) = this.selectRandomVector(entity)

            /* Aggregation. */
            val (time2, mean, plan2) = this.executeMeanQuery(entity, queryVector, parallel, indexType)

            /* Range search. */
            val (time3, r3: List<String>, plan3) = this.executeRangeQuery(entity, queryVector, mean, this.k, parallel, indexType)
            val gt3 = this.executeRangeQuery(entity, queryVector, mean, this.k, 1).second

            /* Range search. */
            val (time4, r4: List<String>, plan4) = this.executeNNSQuery(entity, queryVector, mean, this.k, parallel, indexType)
            val gt4 = this.executeNNSQuery(entity, queryVector, mean, this.k, 1).second

            /* IN query. */
            val (time5, r5, plan5) = this.executeSelectIn(r3 + r4, parallel)
            val gt5 = this.executeSelectIn(r3 + r4, 1).second

            /* Record the query execution plans (one) per type of query! */
            if (r == 0) {
                for ((q,p) in QUERIES.zip(arrayOf(plan1, plan2, plan3, plan4, plan5))) {
                    (this.plans[ENTITY_KEY] as MutableList<String>) += entity
                    (this.plans[INDEX_KEY] as MutableList<String>) += indexType ?: "SCAN"
                    (this.plans[QUERY_KEY] as MutableList<String>) += q
                    (this.plans[PLAN_KEY] as MutableList<List<String>>) += p
                }
            }

            /* Record the raw data. */
            for ((q, data) in QUERIES.zip(arrayOf(emptyList<String>() to emptyList(), emptyList<String>() to emptyList(), r3 to gt3, r4 to gt4, r5 to gt5))) {
                (this.data[ENTITY_KEY] as MutableList<String>) += entity
                (this.data[QUERY_KEY] as MutableList<String>) += q
                (this.data[INDEX_KEY] as MutableList<String>) += indexType ?: "SCAN"
                (this.data[PARALLEL_KEY] as MutableList<Int>) += parallel
                (this.data[RUN_KEY] as MutableList<Int>) += (r + 1)
                (this.data[RESULTS_KEY] as MutableList<List<String>>) += data.first
                (this.data[GROUNDTRUTH_KEY] as MutableList<List<String>>) +=  data.second
                (this.data[K_KEY] as MutableList<Int>) += this.k
            }

            /* Record measurements. */
            for ((q, t) in QUERIES.zip(arrayOf(time1, time2, time3, time4, time5))) {

                /* Record measurements. */
                (this.measurements[ENTITY_KEY] as MutableList<String>) += entity
                (this.measurements[QUERY_KEY] as MutableList<String>) += q
                (this.measurements[INDEX_KEY] as MutableList<String>) += indexType ?: "SCAN"
                (this.measurements[PARALLEL_KEY] as MutableList<Int>) += parallel
                (this.measurements[RUN_KEY] as MutableList<Int>) += (r + 1)
                (this.measurements[K_KEY] as MutableList<Int>) += this.k
                (this.measurements[RUNTIME_KEY] as MutableList<Double>) +=  t / 1000.0

                when(q) {
                    QUERIES[2] -> {
                        (this.measurements[RECALL_KEY] as MutableList<Double>) += Measures.recall(gt3, r3)
                        (this.measurements[DCG_KEY] as MutableList<Double>) +=  Measures.ndcg(gt3, r3)

                        /* Record data. */
                        (this.data[ENTITY_KEY] as MutableList<String>) += entity
                        (this.data[QUERY_KEY] as MutableList<String>) += q
                        (this.data[INDEX_KEY] as MutableList<String>) += indexType ?: "SCAN"
                        (this.data[PARALLEL_KEY] as MutableList<Int>) += parallel
                        (this.data[RUN_KEY] as MutableList<Int>) += (r + 1)
                        (this.data[RESULTS_KEY] as MutableList<List<String>>) += r3
                        (this.data[GROUNDTRUTH_KEY] as MutableList<List<String>>) +=  gt3
                        (this.data[K_KEY] as MutableList<Int>) += this.k
                    }
                    QUERIES[3] -> {
                        (this.measurements[RECALL_KEY] as MutableList<Double>) += Measures.recall(gt4, r4)
                        (this.measurements[DCG_KEY] as MutableList<Double>) +=  Measures.ndcg(gt4, r4)

                        /* Record data. */
                        (this.data[ENTITY_KEY] as MutableList<String>) += entity
                        (this.data[QUERY_KEY] as MutableList<String>) += q
                        (this.data[INDEX_KEY] as MutableList<String>) += indexType ?: "SCAN"
                        (this.data[PARALLEL_KEY] as MutableList<Int>) += parallel
                        (this.data[RUN_KEY] as MutableList<Int>) += (r + 1)
                        (this.data[RESULTS_KEY] as MutableList<List<String>>) += r4
                        (this.data[GROUNDTRUTH_KEY] as MutableList<List<String>>) +=  gt4
                        (this.data[K_KEY] as MutableList<Int>) += this.k
                    }
                    QUERIES[4] -> {
                        (this.measurements[RECALL_KEY] as MutableList<Double>) += Measures.recall(gt5, r5)
                        (this.measurements[DCG_KEY] as MutableList<Double>) +=  Measures.ndcg(gt5, r5)


                        /* Record data. */
                        (this.data[ENTITY_KEY] as MutableList<String>) += entity
                        (this.data[QUERY_KEY] as MutableList<String>) += q
                        (this.data[INDEX_KEY] as MutableList<String>) += indexType ?: "SCAN"
                        (this.data[PARALLEL_KEY] as MutableList<Int>) += parallel
                        (this.data[RUN_KEY] as MutableList<Int>) += (r + 1)
                        (this.data[RESULTS_KEY] as MutableList<List<String>>) += r5
                        (this.data[GROUNDTRUTH_KEY] as MutableList<List<String>>) +=  gt5
                        (this.data[K_KEY] as MutableList<Int>) += this.k
                    }
                    else -> { /* No op. */ }
                }
            }
        }
    }

    /**
     * Selects and returns a random vector from the collection.
     *
     * @param entity The entity to select the vector from.
     */
    private fun selectRandomVector(entity: String): Triple<Long,FloatArray,List<String>> {
        val skip = this.random.nextInt(2000000)
        val query = Query("cineast.${entity}").select("feature").skip(skip.toLong()).limit(1)

        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach { plan.add(it.asString("comment")!!) }
        val queryVector: FloatArray
        val time = measureTimeMillis {
            this.client.query(query).use {
                val tuple = it.next()
                queryVector = tuple.asFloatVector("feature")!!
            }
        }
        return Triple(time, queryVector, plan)
    }

    /**
     * Executes an NNS query that applies a Boolean filter first.
     */
    private fun executeMeanQuery(entity: String, feature: FloatArray, parallel: Int, indexType: String? = null): Triple<Long,Double,List<String>> {
        var query = Query("cineast.${entity}").distance("feature", feature, Distances.L2, "distance").mean()
        query = query.limitParallelism(parallel)
        if (indexType == null) {
            query.disallowIndex()
        } else {
            query.useIndexType(indexType)
        }
        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach { plan.add(it.asString("comment")!!) }

        /* Retrieve results. */
        val mean: Double
        val time = measureTimeMillis {
            this.client.query(query).use {
                mean = it.next().asDouble("distance")!!
            }
        }
        return Triple(time, mean, plan)
    }

    /**
     * Executes a range query.
     *
     * @param entity The entity to search.
     * @param queryVector The query vector.
     * @param k The level parameter k.
     * @param parallel The level of parallelisation.
     * @param indexType The index to use.
     */
    private fun executeRangeQuery(entity: String, queryVector: FloatArray, mean: Double, k: Int, parallel: Int, indexType: String? = null): Triple<Long,List<String>,List<String>> {
        var query = Query("cineast.${entity}")
            .select("id")
            .distance("feature", queryVector, Distances.L2, "distance")
            .where(Expression("distance", "BETWEEN", listOf(mean/2, mean)))
            .order("distance", Direction.ASC)
            .limit(k.toLong())

        /* Parametrise. */
        query = query.limitParallelism(parallel)
        if (indexType == null) {
            query.disallowIndex()
        } else {
            query.useIndexType(indexType)
        }

        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach { plan.add(it.asString("comment")!!) }

        /* Retrieve results. */
        val results = ArrayList<String>(this.k)
        val time = measureTimeMillis {
            this.client.query(query).forEach { t -> results.add(t.asString("id")!!) }
        }
        return Triple(time, results, plan)
    }

    /**
     * Executes a nearest neighbour search query.
     *
     * @param entity The entity to search.
     * @param queryVector The query vector.
     * @param k The level parameter k.
     * @param parallel The level of parallelisation.
     * @param indexType The index to use.
     */
    private fun executeNNSQuery(entity: String, queryVector: FloatArray, mean: Double, k: Int, parallel: Int, indexType: String? = null): Triple<Long,List<String>,List<String>> {
        var query = Query("cineast.${entity}")
            .select("id")
            .distance("feature", queryVector, Distances.L2, "distance")
            .order("distance", Direction.ASC)
            .limit(k.toLong())

        /* Parametrise. */
        query = query.limitParallelism(parallel)
        if (indexType == null) {
            query.disallowIndex()
        } else {
            query.useIndexType(indexType)
        }

        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach { plan.add(it.asString("comment")!!) }

        /* Retrieve results. */
        val results = ArrayList<String>(this.k)
        val time = measureTimeMillis {
            this.client.query(query).forEach { t -> results.add(t.asString("id")!!) }
        }
        return Triple(time, results, plan)
    }

    /**
     * Executes a SELECT * FROM cineast.cineast_segment WHERE segmentid IN () query.
     *
     * @param parallel The level of parallelisation.
     */
    private fun executeSelectIn(ids: List<String>, parallel: Int): Triple<Long,List<String>,List<String>> {
        val query = Query("cineast.cineast_segment")
            .select("*")
            .where(Expression("segmentid", "IN", ids))
            .limitParallelism(parallel)

        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach { plan.add(it.asString("comment")!!) }

        /* Retrieve results. */
        val results = ArrayList<String>(this.k)
        val time = measureTimeMillis {
            this.client.query(query).forEach { t ->
                results.add(t.asString("segmentid")!!)
            }
        }
        return Triple(time, results, plan)
    }
}