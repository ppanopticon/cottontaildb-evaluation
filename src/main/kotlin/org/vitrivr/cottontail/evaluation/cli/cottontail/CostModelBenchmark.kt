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
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import kotlin.collections.ArrayList

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class CostModelBenchmark(private val client: SimpleClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "cost", help = "Executes the cost model simulation for Cottontail DB 'analytics' workload.")  {
    companion object {
        private const val DIGEST_KEY = "digest"
        private const val SCORE_KEY = "score"
        private const val RANK_KEY = "rank"
        private const val PLAN_KEY = "plan"
        private const val QUERY_KEY = "query"
        private const val ENTITY_KEY = "entity"
        private const val CPU_WEIGHT_KEY = "cpu"
        private const val IO_WEIGHT_KEY = "io"
        private const val QUALITY_WEIGHT_KEY = "quality"

        /** List of entities that should be queried. */
        private val ENTITIES = listOf(
            "features_hogmf25k512"
        )

        /** List of index structures that should be used. */
        private val QUERIES = listOf("Mean", "Range", "NNS")
    }

    /** Flag that can be used to directly provide confirmation. */
    private val k: Int by option("-k", "--k", help = "If set, then only the output will be plot.").int().default(1000)

    /** A [SplittableRandom] to generate categories. */
    private val random = SplittableRandom()

    /** Data frame that holds the query plans. */
    private val plans: MutableMap<String,List<*>> = mutableMapOf()

    /** Progress bar used*/
    private var progress: ProgressBar? = null

    /** */
    private var test: MutableMap<Long,MutableList<Pair<Int,Float>>> = mutableMapOf()

    /**
     * Executes the command.
     */
    override fun run() {
        /* Execute benchmark unless plot flag has been set. */
        val out = this.name?.let { this.workingDirectory.resolve("out/${it}") } ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
        if (!Files.exists(out)) {
            Files.createDirectories(out)
        }

        /* Clear plans map. */
        this.plans.clear()
        this.plans[ENTITY_KEY] = mutableListOf<String>()
        this.plans[QUERY_KEY] = mutableListOf<String>()
        this.plans[PLAN_KEY] =  mutableListOf<List<String>>()
        this.plans[RANK_KEY] =  mutableListOf<List<Int>>()
        this.plans[SCORE_KEY] =  mutableListOf<List<Float>>()
        this.plans[DIGEST_KEY] =  mutableListOf<List<Long>>()
        this.plans[CPU_WEIGHT_KEY] = mutableListOf<Float>()
        this.plans[IO_WEIGHT_KEY] = mutableListOf<Float>()
        this.plans[QUALITY_WEIGHT_KEY] = mutableListOf<Float>()

        try {
            /* Initialise progress bar. */
            this.progress = ProgressBarBuilder()
                .setInitialMax((10 * 10 * 10 * ENTITIES.size).toLong())
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
        var cpu = 0.0f
        for (i in 0 until 10) {
            var io = 0.0f
            for (j in 0 until 10) {
                var quality = 0.0f
                for (k in 0 until 10) {
                    this.progress!!.step()

                    /* Random vector. */
                    val queryVector = this.selectRandomVector(entity)

                    /* Aggregation. */
                    val plan1 = this.executeMeanQuery(entity, queryVector, cpu, io, quality)

                    /* Range search. */
                    val plan2 = this.executeRangeQuery(entity, queryVector, 1.0, this.k, cpu, io, quality)

                    /* NNS search. */
                    val plan3 = this.executeNNSQuery(entity, queryVector, this.k, cpu, io, quality)

                    /* Record the query execution plans (one) per type of query! */
                    for ((q,p) in QUERIES.zip(arrayOf(plan1, plan2, plan3))) {
                        for (l in p.indices) {
                            (this.plans[ENTITY_KEY] as MutableList<String>) += entity
                            (this.plans[QUERY_KEY] as MutableList<String>) += q
                            (this.plans[CPU_WEIGHT_KEY] as MutableList<Float>) += cpu
                            (this.plans[IO_WEIGHT_KEY] as MutableList<Float>) += io
                            (this.plans[QUALITY_WEIGHT_KEY] as MutableList<Float>) += quality
                            (this.plans[DIGEST_KEY] as MutableList<Long>) += p[l].first
                            (this.plans[PLAN_KEY] as MutableList<String>) += p[l].second
                            (this.plans[SCORE_KEY] as MutableList<Float>) += p[l].third
                            (this.plans[RANK_KEY] as MutableList<Int>) += l + 1
                        }
                    }
                    quality += 0.1f
                }
                io += 0.1f
            }
            cpu += 0.1f
        }
    }

    /**
     * Selects and returns a random vector from the collection.
     *
     * @param entity The entity to select the vector from.
     */
    private fun selectRandomVector(entity: String): FloatArray {
        val skip = this.random.nextInt(2000000)
        val query = Query("cineast.${entity}").select("feature").skip(skip.toLong()).limit(1)

        /* Retrieve execution plan. */
        val queryVector: FloatArray
        this.client.query(query).use {
            val tuple = it.next()
            queryVector = tuple.asFloatVector("feature")!!
        }
        return queryVector
    }

    /**
     * Executes an NNS query that applies a Boolean filter first.
     */
    private fun executeMeanQuery(entity: String, feature: FloatArray, cpuWeight: Float, ioWeight: Float, qualityWeight: Float): List<Triple<Long,String,Float>> {
        val query = Query("cineast.${entity}")
            .distance("feature", feature, Distances.L2, "distance")
            .mean()
            .usePolicy(wcpu = cpuWeight, wio = ioWeight, wq = qualityWeight, wmem = 0.0f)

        /* Retrieve execution plan. */
        val plans = ArrayList<Triple<Long,String,Float>>(this.k)
        this.client.explain(query).forEach {
            if (it.asInt("position") == 1) {
                if (!this.test.containsKey(it.asLong("digest")!!)) {
                    this.test[it.asLong("digest")!!] = mutableListOf()
                }
                plans.add(Triple(it.asLong("digest")!!, it.asString("designation")!!, it.asFloat("score")!!))
            }
            println(it)
        }
        return plans
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
    private fun executeRangeQuery(entity: String, queryVector: FloatArray, mean: Double, k: Int, cpuWeight: Float, ioWeight: Float, qualityWeight: Float): List<Triple<Long,String,Float>> {
        val query = Query("cineast.${entity}")
            .select("id")
            .distance("feature", queryVector, Distances.L2, "distance")
            .where(Expression("distance", "BETWEEN", listOf(mean/2, mean)))
            .order("distance", Direction.ASC)
            .limit(k.toLong())
            .usePolicy(wcpu = cpuWeight, wio = ioWeight, wq = qualityWeight, wmem = 0.0f)

        /* Retrieve execution plan. */
        val plans = ArrayList<Triple<Long,String,Float>>(this.k)
        this.client.explain(query).forEach {
            if (it.asInt("position") == 1) {
                if (!this.test.containsKey(it.asLong("digest")!!)) {
                    this.test[it.asLong("digest")!!] = mutableListOf()
                }
                plans.add(Triple(it.asLong("digest")!!, it.asString("designation")!!, it.asFloat("score")!!))
            }
            println(it)
        }
        return plans
    }

    /**
     * Executes a nearest neighbour search query.
     *
     * @param entity The entity to search.
     * @param queryVector The query vector.
     * @param k The level parameter k.
     * @param qualityWeight The weight of the quality cost policy paarameter
     */
    private fun executeNNSQuery(entity: String, queryVector: FloatArray, k: Int, cpuWeight: Float, ioWeight: Float, qualityWeight: Float): List<Triple<Long,String,Float>> {
        val query = Query("cineast.${entity}")
            .select("id")
            .distance("feature", queryVector, Distances.L2, "distance")
            .order("distance", Direction.ASC)
            .limit(k.toLong())
            .usePolicy(wcpu = cpuWeight, wio = ioWeight, wq = qualityWeight, wmem = 0.0f)

        /* Retrieve execution plan. */
        val plans = ArrayList<Triple<Long,String,Float>>(this.k)
        this.client.explain(query).forEach {
            if (it.asInt("position") == 1) {
                if (!this.test.containsKey(it.asLong("digest")!!)) {
                    this.test[it.asLong("digest")!!] = mutableListOf()
                }
                plans.add(Triple(it.asLong("digest")!!, it.asString("designation")!!, it.asFloat("score")!!))
            }
            println(it)
        }
        return plans
    }
}