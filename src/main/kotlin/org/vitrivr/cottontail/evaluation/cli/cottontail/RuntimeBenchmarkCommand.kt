package org.vitrivr.cottontail.evaluation.cli.cottontail

import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import jetbrains.letsPlot.asDiscrete
import jetbrains.letsPlot.export.ggsave
import jetbrains.letsPlot.facet.facetGrid
import jetbrains.letsPlot.geom.geomBoxplot
import jetbrains.letsPlot.geom.geomErrorBar
import jetbrains.letsPlot.geom.geomPoint
import jetbrains.letsPlot.ggsize
import jetbrains.letsPlot.label.labs
import jetbrains.letsPlot.letsPlot
import jetbrains.letsPlot.scale.ylim
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.evaluation.constants.*
import org.vitrivr.cottontail.evaluation.datasets.YandexDeep1BIterator
import org.vitrivr.cottontail.evaluation.utilities.Measures
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.system.measureTimeMillis

/**
 * Performs a simple runtime measurement for a series of queries.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RuntimeBenchmarkCommand(private val client: SimpleClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "runtime", help = "Prepares and loads all data required for Cottontail DB benchmarks.")  {

    /** Flag that can be used to directly provide confirmation. */
    private val k: Int by option("-k", "--k", help = "If set, then only the output will be plot.").int().default(1000)

    /** Data frame that holds the data. */
    private var data = mapOf<String,MutableList<*>>(
        ENTITY_KEY to mutableListOf<String>(),
        INDEX_KEY to mutableListOf<String>(),
        PARALLEL_KEY to mutableListOf<Int>(),
        TYPE_KEY to mutableListOf<String>(),
        RUN_KEY to mutableListOf<Int>(),
        K_KEY to mutableListOf<Int>(),
        RUNTIME_KEY to mutableListOf<Double>(),
        DCG_KEY to mutableListOf<Double>(),
        RECALL_KEY to mutableListOf<Double>()
    )

    /**
     * Executes the command.
     */
    override fun run() {
        /* Execute benchmark unless plot flag has been set. */
        if (this.plotOnly) {
            /* Determine and generate output directory. */
            val out = this.output ?: Files.walk(this.workingDirectory.resolve("out")).filter { Files.isDirectory(it) }.sorted(Comparator.reverseOrder()).findFirst().orElseThrow {
                IllegalStateException("Could not identify a most recent output directory. Please specify output directory to plot.")
            }
            this.data = Files.newBufferedReader(out.resolve("data.json")).use { Gson().fromJson(it, Map::class.java) } as Map<String,MutableList<Float>>

            /* Generate plots. */
            this.plot(out)
        } else {
            /* Determine and generate output directory. */
            val out = this.output ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
            if (!Files.exists(out)) {
                Files.createDirectories(out)
            }

            /* Yandex Deep 5M Series. */
            this.runYandexDeep1B("yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2)
            this.runYandexDeep1B("yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2, indexType = "VAF")
            this.runYandexDeep1B("yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2, indexType = "PQ")
            this.runYandexDeep1B("yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4)
            this.runYandexDeep1B("yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4, indexType = "VAF")
            this.runYandexDeep1B("yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4, indexType = "PQ")
            this.runYandexDeep1B("yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8)
            this.runYandexDeep1B("yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8, indexType = "VAF")
            this.runYandexDeep1B("yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8, indexType = "PQ")

            /* Yandex Deep 10M Series. */
            this.runYandexDeep1B("yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2)
            this.runYandexDeep1B("yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2, indexType = "VAF")
            this.runYandexDeep1B("yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2, indexType = "PQ")
            this.runYandexDeep1B("yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4)
            this.runYandexDeep1B("yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4, indexType = "VAF")
            this.runYandexDeep1B("yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4, indexType = "PQ")
            this.runYandexDeep1B("yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8)
            this.runYandexDeep1B("yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8, indexType = "VAF")
            this.runYandexDeep1B("yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8, indexType = "PQ")

            /* Yandex Deep 100M Series. */
            this.runYandexDeep1B("yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2)
            this.runYandexDeep1B("yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2, indexType = "VAF")
            this.runYandexDeep1B("yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2, indexType = "PQ")
            this.runYandexDeep1B("yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4)
            this.runYandexDeep1B("yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4, indexType = "VAF")
            this.runYandexDeep1B("yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4, indexType = "PQ")
            this.runYandexDeep1B("yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8)
            this.runYandexDeep1B("yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8, indexType = "VAF")
            this.runYandexDeep1B("yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8, indexType = "PQ")

            /*this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2)
            this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4)
            this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8)
            this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 16)
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32)

            /* Yandex Deep 5M Series. */
            this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 1, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 16, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32, indexType = "VAF")

            /* Yandex Deep 10M Series. */
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 1)
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2)
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4)
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8)
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 16)
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32)

            /* Yandex Deep 10M Series. */
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 1, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 16, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32, indexType = "VAF")


            /* Yandex Deep 10M Series. */
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 1)
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2)
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4)
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8)
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 16)
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32)

            /* Yandex Deep 10M Series. */
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 1, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 2, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 4, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 8, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 16, indexType = "VAF")
            this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 32, indexType = "VAF")


            //this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 1, "VAF")
            //this.runYandexDeep1B("evaluation.yandex_deep5M", k = this.k, warmup = this.warmup, iterations = this.repeat, parallel = 3, "VAF")

            /* Yandex Deep 10M Series. */
            //this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = true)
            //this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = false)
            //this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = true, "idx_feature_vaf")
            //this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = true, "idx_feature_pq")

            /* Yandex Deep 10M Series. */
            //this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = true)
            //this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = false)
            //this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = true, "idx_feature_vaf")
            //this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = true, "idx_feature_pq")

            /* Yandex Deep 1B Series. */
            //this.runYandexDeep1B("evaluation.yandex_deep10M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = this.noParallel, this.index)
            //this.runYandexDeep1B("evaluation.yandex_deep100M", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = this.noParallel, this.index)
            //this.runYandexDeep1B("evaluation.yandex_deep1B", k = this.k, warmup = this.warmup, iterations = this.repeat, noParallel = this.noParallel, this.index)
            */
            /* Export raw data. */
            this.export(out)

            /* Generate plots. */
            this.plot(out)
        }


    }

    override fun plot(out: Path) {
        /* Prepare plot. */
        var runtimePlot = letsPlot(data)
        runtimePlot += geomBoxplot(color = "#A5D7D2", fill = "#D2EBE9", outlierColor = "#D20537", showLegend = false) { x = asDiscrete(TYPE_KEY); y = RUNTIME_KEY; }
        runtimePlot += labs(x = "", y = "Runtime [s]")
        runtimePlot += ylim(listOf(0.0, 10.0))
        runtimePlot += facetGrid(ENTITY_KEY)
        runtimePlot += ggsize(1000, 500)

        /* Recall plot. */
        var qualityPlot = letsPlot(data)
        qualityPlot += geomPoint(color = "#A5D7D2") { x = asDiscrete(TYPE_KEY); y = RECALL_KEY}
        qualityPlot += labs(x = "", y = "Quality")
        qualityPlot += ylim(listOf(0.0, 1.0))
        qualityPlot += ggsize(1000, 500)

        /* Export plot as PNG. */
        ggsave(runtimePlot, filename = "runtime.png", path = out.toString())
        ggsave(qualityPlot, filename = "recall.png", path = out.toString())
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
        val type = "$indexName (p=$parallel)"
        val bar = ProgressBarBuilder().setInitialMax((warmup + iterations).toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Running Benchmark ($type)").build()
        YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use {
            /* Warmup query. */
            bar.extraMessage = "Warmup..."
            for (w in 0 until warmup) {
                bar.step()
                val (_, feature) = it.next()
                executeQuery(entity, feature, k, parallel, indexType)
            }

            // Benchmark query.
            bar.setExtraMessage("Benchmark...")
            for (w in 0 until iterations) {
                /* Indicate progress. */
                bar.step()

                val (_, feature) = it.next()
                val gt = executeQuery(entity, feature, k, parallel)
                val result = executeQuery(entity, feature, k, parallel, indexType)

                /* Record data. */
                (this.data[ENTITY_KEY] as MutableList<String>).add(entity.replace("yandex_", "Yandex "))
                (this.data[INDEX_KEY] as MutableList<String>).add(indexName)
                (this.data[PARALLEL_KEY] as MutableList<Int>).add(parallel)
                (this.data[TYPE_KEY] as MutableList<String>).add(type)
                (this.data[RUN_KEY] as MutableList<Int>).add(w + 1)
                (this.data[K_KEY] as MutableList<Int>).add(k)
                (this.data[RUNTIME_KEY] as MutableList<Double>).add(result.first / 1000.0)
                (this.data[DCG_KEY] as MutableList<Double>).add(Measures.ndcg(gt.second, result.second))
                (this.data[RECALL_KEY] as MutableList<Double>).add(Measures.recall(gt.second, result.second))
            }
        }
    }

    /**
     * Executes a query.
     */
    private fun executeQuery(entity: String, feature: FloatArray, k: Int = 10, parallel: Int = 2, indexType: String? = null): Pair<Long,List<Int>> {
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
        val duration = measureTimeMillis {
            this.client.query(query).forEach { t ->
                results.add(t.asInt("id")!!)
            }
        }

        return duration to results
    }
}
