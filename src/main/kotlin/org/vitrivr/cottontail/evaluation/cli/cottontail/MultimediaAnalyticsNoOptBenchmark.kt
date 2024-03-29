package org.vitrivr.cottontail.evaluation.cli.cottontail

import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
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
import org.vitrivr.cottontail.evaluation.cli.AbstractBenchmarkCommand
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import kotlin.collections.ArrayList
import kotlin.system.measureTimeMillis

/**
 * A variant of the analytics benchmarks w/o certain planner rules used to benchmark Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class MultimediaAnalyticsNoOptBenchmark (private val client: SimpleClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "optimisation", help = "Executes to Cottontail DB 'analytics' workload without optimisation.")  {
    companion object {
        private const val PLAN_KEY = "plan"
        private const val QUERY_KEY = "query"
        private const val ENTITY_KEY = "entity"
        private const val RUN_KEY = "run"
        private const val K_KEY = "k"
        private const val RUNTIME_KEY = "runtime"
        private const val DCG_KEY = "ndcg"
        private const val RECALL_KEY = "recall"
        private const val RESULTS_KEY = "results"
        private const val GROUNDTRUTH_KEY = "groundtruth"

        /** List of entities that should be queried. */
        private val ENTITIES = listOf(
            "features_averagecolor",
            "features_visualtextcoembedding",
            "features_hogmf25k512",
            "features_inceptionresnetv2",
            "features_conceptmasksade20k"
        )

        /** List of parallelism levels that should be tested. */
        private val PARALLEL = listOf(1)

        /** List of index structures that should be used. */
        private val QUERIES = listOf("Fetch", "Mean", "Range", "NNS", "Select")
    }

    /** Flag that can be used to directly provide confirmation. */
    private val k: Int by option("-k", "--k", help = "If set, then only the output will be plot.").int().default(1000)

    /** Flag that can be used to directly provide confirmation. */
    private val optimise: Boolean by option("-o", "--opt", help = "If set, then only the output will be plot.").flag("--no-opt", "-n", default = true)

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
        this.measurements[K_KEY] = mutableListOf<Int>()
        this.measurements[RUN_KEY] = mutableListOf<Int>()
        this.measurements[RUNTIME_KEY] = mutableListOf<Double>()
        this.measurements[DCG_KEY] = mutableListOf<Double>()
        this.measurements[RECALL_KEY] = mutableListOf<Double>()

        /* Clear data map. */
        this.data.clear()
        this.data[ENTITY_KEY] = mutableListOf<String>()
        this.data[QUERY_KEY] = mutableListOf<String>()
        this.data[RUN_KEY] = mutableListOf<Int>()
        this.data[K_KEY] = mutableListOf<Int>()
        this.data[RESULTS_KEY] = mutableListOf<List<String>>()
        this.data[GROUNDTRUTH_KEY] = mutableListOf<List<String>>()

        /* Clear plans map. */
        this.plans.clear()
        this.plans[ENTITY_KEY] = mutableListOf<String>()
        this.plans[QUERY_KEY] = mutableListOf<String>()
        this.plans[PLAN_KEY] =  mutableListOf<List<String>>()


        try {
            /* Initialise progress bar. */
            this.progress = ProgressBarBuilder()
                .setInitialMax((ENTITIES.size * this.repeat).toLong())
                .setStyle(ProgressBarStyle.ASCII).setTaskName("Multimedia Analytics Benchmark:").build()

            /* Execute workload. */
            for (entity in ENTITIES) {
                executeWorkload(entity)
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

        /* Export raw data. */
        Files.newBufferedWriter(out.resolve("plans.json"), StandardOpenOption.CREATE_NEW).use {
            val gson = GsonBuilder().setPrettyPrinting().create()
            gson.toJson(this.plans, Map::class.java, gson.newJsonWriter(it))
        }
    }

    /**
     * Executes a workload. This is the equivalent of executing a single round of queries.
     *
     * @param entity Name of the entity to search.
     */
    private fun executeWorkload(entity: String) {
        /** */
        for (r in 0 until this.repeat) {
            this.progress!!.step()

            /* Random vector. */
            val (time1, queryVector, plan1) = this.selectRandomVector(entity)

            /* Aggregation. */
            val (time2, mean, plan2) = this.executeMeanQuery(entity, queryVector)

            /* Range search. */
            val (time3, r3: List<String>, plan3) = this.executeRangeQuery(entity, queryVector, mean)

            /* NNS search. */
            val (time4, r4: List<String>, plan4) = this.executeNNSQuery(entity, queryVector)

            /* IN query. */
            val (time5, r5, plan5) = this.executeSelectIn(r3 + r4)

            /* Record the query execution plans (one) per type of query! */
            if (r == 0) {
                for ((q,p) in QUERIES.zip(arrayOf(plan1, plan2, plan3, plan4, plan5))) {
                    (this.plans[ENTITY_KEY] as MutableList<String>) += entity
                    (this.plans[QUERY_KEY] as MutableList<String>) += q
                    (this.plans[PLAN_KEY] as MutableList<List<String>>) += p
                }
            }

            /* Record measurements. */
            for ((q, t) in QUERIES.zip(arrayOf(time1, time2, time3, time4, time5))) {

                /* Record measurements. */
                (this.measurements[ENTITY_KEY] as MutableList<String>) += entity
                (this.measurements[QUERY_KEY] as MutableList<String>) += q
                (this.measurements[RUN_KEY] as MutableList<Int>) += (r + 1)
                (this.measurements[K_KEY] as MutableList<Int>) += this.k
                (this.measurements[RUNTIME_KEY] as MutableList<Double>) +=  t / 1000.0

                when(q) {
                    QUERIES[2] -> {
                        /* Record data. */
                        (this.data[ENTITY_KEY] as MutableList<String>) += entity
                        (this.data[QUERY_KEY] as MutableList<String>) += q
                        (this.data[RUN_KEY] as MutableList<Int>) += (r + 1)
                        (this.data[RESULTS_KEY] as MutableList<List<String>>) += r3
                        (this.data[K_KEY] as MutableList<Int>) += this.k
                    }
                    QUERIES[3] -> {
                        /* Record data. */
                        (this.data[ENTITY_KEY] as MutableList<String>) += entity
                        (this.data[QUERY_KEY] as MutableList<String>) += q
                        (this.data[RUN_KEY] as MutableList<Int>) += (r + 1)
                        (this.data[RESULTS_KEY] as MutableList<List<String>>) += r4
                        (this.data[K_KEY] as MutableList<Int>) += this.k
                    }
                    QUERIES[4] -> {
                        /* Record data. */
                        (this.data[ENTITY_KEY] as MutableList<String>) += entity
                        (this.data[QUERY_KEY] as MutableList<String>) += q
                        (this.data[RUN_KEY] as MutableList<Int>) += (r + 1)
                        (this.data[RESULTS_KEY] as MutableList<List<String>>) += r5
                        (this.data[K_KEY] as MutableList<Int>) += this.k
                    }
                    else -> {
                        (this.measurements[RECALL_KEY] as MutableList<Double>) += 1.0
                        (this.measurements[DCG_KEY] as MutableList<Double>) += 1.0
                    }
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
        var query = Query("cineast.${entity}")
            .select("feature")
            .skip(skip.toLong())
            .limit(1)
            .disallowParallelism()

        if (!this.optimise) {
            query = query.disallowOptimisation()
        }

        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach {
            if (it.asInt("rank") == 1) {
                plan.add(it.asString("designation")!!)
            }
        }
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
    private fun executeMeanQuery(entity: String, feature: FloatArray): Triple<Long,Double,List<String>> {
        var query = Query("cineast.${entity}")
            .distance("feature", feature, Distances.L2, "distance")
            .mean()
            .disallowParallelism()

        query = if (!this.optimise) {
            query.disallowOptimisation()
        } else {
            query.disallowIndex()
        }

        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach {
            if (it.asInt("rank") == 1) {
                plan.add(it.asString("designation")!!)
            }
        }

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
    private fun executeRangeQuery(entity: String, queryVector: FloatArray, mean: Double): Triple<Long,List<String>,List<String>> {
        var query = Query("cineast.${entity}")
            .select("id")
            .distance("feature", queryVector, Distances.L2, "distance")
            .where(Expression("distance", "BETWEEN", listOf(mean/2, mean)))
            .order("distance", Direction.ASC)
            .limit(this.k.toLong())
            .disallowParallelism()

        query = if (!this.optimise) {
            query.disallowOptimisation()
        } else {
            query.disallowIndex()
        }

        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach {
            if (it.asInt("rank") == 1) {
                plan.add(it.asString("designation")!!)
            }
        }

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
     */
    private fun executeNNSQuery(entity: String, queryVector: FloatArray): Triple<Long,List<String>,List<String>> {
        var query = Query("cineast.${entity}")
            .select("id")
            .distance("feature", queryVector, Distances.L2, "distance")
            .order("distance", Direction.ASC)
            .limit(this.k.toLong())
            .disallowParallelism()

        query = if (!this.optimise) {
            query.disallowOptimisation()
        } else {
            query.disallowIndex()
        }

        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach {
            if (it.asInt("rank") == 1) {
                plan.add(it.asString("designation")!!)
            }
        }

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
    private fun executeSelectIn(ids: List<String>): Triple<Long,List<String>,List<String>> {
        var query = Query("cineast.cineast_segment")
            .select("*")
            .where(Expression("segmentid", "IN", ids))
            .disallowParallelism()

        query = if (!this.optimise) {
            query.disallowOptimisation()
        } else {
            query.disallowIndex()
        }

        /* Retrieve execution plan. */
        val plan = ArrayList<String>(this.k)
        this.client.explain(query).forEach {
            if (it.asInt("rank") == 1) {
                plan.add(it.asString("designation")!!)
            }
        }

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