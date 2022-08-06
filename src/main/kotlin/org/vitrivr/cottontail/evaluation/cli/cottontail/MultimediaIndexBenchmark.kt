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
 * @version 1.0
 */
class MultimediaIndexBenchmark(private val client: SimpleClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "index", help = "Tests index performance for NNS.")  {
    companion object {
        private const val QUERY_KEY = "query"
        private const val ENTITY_KEY = "entity"
        private const val RUN_KEY = "run"
        private const val K_KEY = "k"
        private const val PARALLEL_KEY = "parallel"
        private const val INDEX_KEY = "index"
        private const val RUNTIME_KEY = "runtime"
        private const val DCG_KEY = "ndcg"
        private const val RECALL_KEY = "recall"

        /** List of entities that should be queried. */
        private val ENTITIES = listOf(
            "features_hogmf25k512",
            "features_inceptionresnetv2",
            "features_conceptmasksade20k"
        )

        /** List of index structures that should be used. */
        private val INDEXES = listOf("VAF")

        /** List of parallelism levels that should be tested. */
        private val PARALLEL = listOf(2, 4, 8, 16, 32)
    }

    /** Flag that can be used to directly provide confirmation. */
    private val k: Int by option("-k", "--k", help = "If set, then only the output will be plot.").int().default(1000)

    /** Flag that can be used to directly provide confirmation. */
    private val p: Int by option("-p", "--parallel", help = "The degree of parallelism to allow. Defaults to 0 (no limit).").int().default(0)

    /** Flag that can be used to directly provide confirmation. */
    private val index: String by option("-i", "--index", help = "The index to test (defautls to VAF).").default("VAF")


    /** A [SplittableRandom] to generate categories. */
    private val random = SplittableRandom()

    /** Data frame that holds the data. */
    private val measurements: MutableMap<String,List<*>> = mutableMapOf()

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

        try {
            /* Initialise progress bar. */
            this.progress = ProgressBarBuilder()
                .setInitialMax((ENTITIES.size  * INDEXES.size * PARALLEL.size * this.repeat).toLong())
                .setStyle(ProgressBarStyle.ASCII).setTaskName("Multimedia Index Benchmark:").build()

            /* Execute workload. */
            for (entity in ENTITIES) {
                executeWorkload(entity, this.p, this.index)
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

            /* NNS search. */
            val (time2, r1: List<String>, plan2) = this.executeNNSQuery(entity, queryVector, this.k, parallel, indexType)

            /* Record measurements. */
            (this.measurements[ENTITY_KEY] as MutableList<String>) += entity
            (this.measurements[INDEX_KEY] as MutableList<String>) += indexType ?: "SCAN"
            (this.measurements[PARALLEL_KEY] as MutableList<Int>) += parallel
            (this.measurements[RUN_KEY] as MutableList<Int>) += (r + 1)
            (this.measurements[K_KEY] as MutableList<Int>) += this.k
            (this.measurements[RUNTIME_KEY] as MutableList<Double>) +=  time2 / 1000.0
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
        this.client.explain(query).forEach { plan.add(it.asString("designation")!!) }
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
     * Executes a nearest neighbour search query.
     *
     * @param entity The entity to search.
     * @param queryVector The query vector.
     * @param k The level parameter k.
     * @param parallel The level of parallelisation.
     * @param indexType The index to use.
     */
    private fun executeNNSQuery(entity: String, queryVector: FloatArray, k: Int, parallel: Int, indexType: String? = null): Triple<Long,List<String>,List<String>> {
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
        this.client.explain(query).forEach { plan.add(it.asString("designation")!!) }

        /* Retrieve results. */
        val results = ArrayList<String>(this.k)
        val time = measureTimeMillis {
            this.client.query(query).forEach { t -> results.add(t.asString("id")!!) }
        }
        return Triple(time, results, plan)
    }
}