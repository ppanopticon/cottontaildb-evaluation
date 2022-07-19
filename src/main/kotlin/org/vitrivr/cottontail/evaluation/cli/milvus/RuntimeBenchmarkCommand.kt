package org.vitrivr.cottontail.evaluation.cli.milvus

import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import io.milvus.client.MilvusServiceClient
import io.milvus.grpc.SearchResults
import io.milvus.param.Constant
import io.milvus.param.MetricType
import io.milvus.param.R
import io.milvus.param.collection.LoadCollectionParam
import io.milvus.param.collection.ReleaseCollectionParam
import io.milvus.param.collection.ShowCollectionsParam
import io.milvus.param.dml.SearchParam
import io.milvus.response.SearchResultsWrapper
import jetbrains.letsPlot.*
import jetbrains.letsPlot.export.ggsave
import jetbrains.letsPlot.facet.facetGrid
import jetbrains.letsPlot.geom.geomBar
import jetbrains.letsPlot.geom.geomErrorBar
import jetbrains.letsPlot.geom.geomPoint
import jetbrains.letsPlot.geom.geomText
import jetbrains.letsPlot.label.labs
import jetbrains.letsPlot.scale.scaleColorManual
import jetbrains.letsPlot.scale.scaleFillManual
import jetbrains.letsPlot.scale.ylim
import jetbrains.letsPlot.tooltips.layerTooltips
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.vitrivr.cottontail.evaluation.cli.cottontail.AbstractBenchmarkCommand
import org.vitrivr.cottontail.evaluation.cli.cottontail.RuntimeBenchmarkCommand
import org.vitrivr.cottontail.evaluation.constants.SCALE_COLORS
import org.vitrivr.cottontail.evaluation.constants.SCALE_FILLS
import org.vitrivr.cottontail.evaluation.constants.THEME
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
class RuntimeBenchmarkCommand(private val client: MilvusServiceClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "runtime", help = "Prepares and loads all data required for Cottontail DB benchmarks.")  {
    companion object {
        private const val RUN_KEY = "run"
        private const val K_KEY = "k"
        private const val RESULTS_KEY = "results"
        private const val GROUNDTRUTH_KEY = "groundtruth"
        private const val RUNTIME_KEY = "runtime"
        private const val RUNTIME_WITH_LOAD_KEY = "runtime_with_load"
        private const val DCG_KEY = "dcg"
        private const val RECALL_KEY = "recall"
    }

    /** Flag that can be used to directly provide confirmation. */
    private val k: Int by option("-k", "--k", help = "If set, then only the output will be plot.").int().default(1000)

    /** Data frame that holds the data. */
    private var data: Map<String,Map<String,List<*>>> = HashMap()

    /**
     * Executes the command.
     */
    override fun run() {
        /* Execute benchmark unless plot flag has been set. */
        if (this.plotOnly) {
            /* Determine and generate output directory. */
            val out = this.output ?: Files.walk(this.workingDirectory.resolve("out")).filter { Files.isDirectory(it) }.sorted(Comparator.reverseOrder()).findFirst().orElseThrow {
                IllegalStateException("")
            }
            this.data = Files.newBufferedReader(out.resolve("data.json")).use { Gson().fromJson(it, Map::class.java) } as MutableMap<String,Map<String,List<*>>>

            /* Generate plots. */
            this.plot(out)
        } else {

            /* Make sure that all collections have been released. */
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep5m").build())
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep10m").build())
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep100m").build())
            //this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep1b").build())

            /* Determine and generate output directory. */
            val out = this.output ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
            if (!Files.exists(out)) {
                Files.createDirectories(out)
            }

            /* Yandex Deep 5M Series. */
            this.runYandexDeep1B("yandex_deep5m", k = this.k, warmup = this.warmup, iterations = this.repeat)
            this.runYandexDeep1B("yandex_deep10m", k = this.k, warmup = this.warmup, iterations = this.repeat)
            this.runYandexDeep1B("yandex_deep100m", k = this.k, warmup = this.warmup, iterations = this.repeat)
            //this.runYandexDeep1B("yandex_deep1b", k = this.k, warmup = this.warmup, iterations = this.repeat)

            /* Export raw data. */
            this.export(out)

            /* Generate plots. */
            this.plot(out)
        }
    }

    override fun plot(out: Path) {

        val runtime = this.data.entries.sortedBy { it.key }.map { (k,v ) ->
            val stats1 = DescriptiveStatistics((v[RUNTIME_KEY] as List<Double>).toDoubleArray())
            val stats2 = DescriptiveStatistics((v[RUNTIME_WITH_LOAD_KEY] as List<Double>).toDoubleArray())
            mapOf(
                "x" to mutableListOf<Any>(k.replace("yandex_deep", "").uppercase(), k.replace("yandex_deep", "").uppercase()),
                "color" to mutableListOf<Any>("Query", "Loading + Query"),
                "y" to mutableListOf<Any>(stats1.mean, stats2.mean),
                "max" to mutableListOf<Any>(stats1.mean + stats1.standardDeviation, stats2.mean + stats2.standardDeviation),
                "min" to mutableListOf<Any>(stats1.mean - stats1.standardDeviation, stats2.mean - stats2.standardDeviation),
                "label_y" to mutableListOf<Any>(stats1.mean + 6.0, stats2.mean + 6.0),
                "label" to mutableListOf<Any>(
                    "${"%.2f".format(stats1.mean)}±${"%.2f".format(stats1.standardDeviation)}",
                    "${"%.2f".format(stats2.mean)}±${"%.2f".format(stats2.standardDeviation)}"
                )

            )
        }.reduce { acc, map ->
            acc["x"]!!.addAll(map["x"]!!)
            acc["color"]!!.addAll(map["color"]!!)
            acc["y"]!!.addAll(map["y"]!!)
            acc["max"]!!.addAll(map["max"]!!)
            acc["min"]!!.addAll(map["min"]!!)
            acc["label_y"]!!.addAll(map["label_y"]!!)
            acc["label"]!!.addAll(map["label"]!!)
            acc
        }

        val quality = this.data.entries.sortedBy { it.key }.map { (k,v) ->
            val stats1 = DescriptiveStatistics((v[RECALL_KEY] as List<Double>).toDoubleArray())
            val stats2 = DescriptiveStatistics((v[DCG_KEY] as List<Double>).toDoubleArray())
            mapOf(
                "x" to mutableListOf<Any>(k.replace("yandex_deep", "").uppercase(), k.replace("yandex_deep", "").uppercase()),
                "color" to mutableListOf<Any>("Recall@${this.k}","DCG@${this.k}"),
                "y" to mutableListOf<Any>(stats1.mean, stats2.mean),
                "max" to mutableListOf<Any>(stats1.mean + stats1.standardDeviation, stats2.mean + stats2.standardDeviation),
                "min" to mutableListOf<Any>(stats1.mean - stats1.standardDeviation, stats2.mean - stats2.standardDeviation),
            )
        }.reduce { acc, map ->
            acc["x"]!!.addAll(map["x"]!!)
            acc["color"]!!.addAll(map["color"]!!)
            acc["y"]!!.addAll(map["y"]!!)
            acc["max"]!!.addAll(map["max"]!!)
            acc["min"]!!.addAll(map["min"]!!)
            acc
        }

        /* Prepare plot. */
        var runtimePlot = letsPlot(runtime) { x = "color"; y = "y"; color = "color"; fill = "color"; }
        runtimePlot += geomBar(position = Pos.dodge, stat = Stat.identity) {  }
        runtimePlot += geomErrorBar(width = 0.1, position = positionDodge(0.9), showLegend = false) { ymax = "max"; ymin = "min"; }
        runtimePlot += geomText(size = 6, position = positionDodge(0.90), showLegend = false) { y = "label_y"; label = "label"; }
        runtimePlot += labs(x = "Mode", y = "Runtime [s]", fill = "Mode", color = "Mode")
        runtimePlot += ylim(listOf(0.0, 200.0))
        runtimePlot += facetGrid("x", xOrder = -1)
        runtimePlot += SCALE_COLORS
        runtimePlot += SCALE_FILLS
        runtimePlot += THEME.legendPosition(1, 1).legendJustification(1, 1).legendDirectionHorizontal()
        runtimePlot += ggsize(1250, 750)

        /* Recall plot. */
        var qualityPlot = letsPlot(quality) { x = "color"; y = "y"; color = "color"; fill = "color"; }
        qualityPlot += geomPoint(position = Pos.dodge, stat = Stat.identity, size = 8)
        qualityPlot += labs(x = "Metric", y = "Quality", color = "Metric", fill = "Metric")
        qualityPlot += ylim(listOf(0.0, 1.0))
        qualityPlot += facetGrid("x", xOrder = -1)
        qualityPlot += SCALE_COLORS
        qualityPlot += SCALE_FILLS
        qualityPlot += THEME.legendPosition(1, 0.25).legendJustification(1, 1).legendDirectionHorizontal()
        qualityPlot += ggsize(1250, 750)

        /* Export plot as PNG. */
        ggsave(runtimePlot, filename = "runtime.png", path = out.toString())
        ggsave(qualityPlot, filename = "quality.png", path = out.toString())
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
    private fun runYandexDeep1B(entity: String, k: Int = 10, warmup: Int = 1, iterations: Int = 10) {
        try {
            /* The local data. */
            val localData = mapOf<String,List<*>>(
                RUN_KEY to mutableListOf<Int>(),
                K_KEY to mutableListOf<Int>(),
                RUNTIME_KEY to mutableListOf<Double>(),
                RUNTIME_WITH_LOAD_KEY to mutableListOf<Double>(),
                DCG_KEY to mutableListOf<Double>(),
                RECALL_KEY to mutableListOf<Double>(),
                RESULTS_KEY to mutableListOf<List<Long>>(),
                GROUNDTRUTH_KEY to mutableListOf<List<Any>>()
            )

            /* Now start benchmark. */
            val bar = ProgressBarBuilder().setInitialMax((warmup + iterations).toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Running Milvus Benchmark...").build()
            YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use {
                /* Warmup query. */
                bar.extraMessage = "Warmup..."
                for (w in 0 until warmup) {
                    bar.step()
                    val (_, feature) = it.next()
                    executeQuery(entity, feature, k)
                }

                // Benchmark query.
                bar.extraMessage = "Benchmark..."
                for (w in 0 until iterations) {
                    /* Indicate progress. */
                    bar.step()

                    val (_, feature) = it.next()
                    val gt = executeQuery(entity, feature, k)
                    val result = executeQuery(entity, feature, k)

                    /* Record data. */
                    (localData[RUN_KEY] as MutableList<Int>).add(w + 1)
                    (localData[K_KEY] as MutableList<Int>).add(k)
                    (localData[RUNTIME_KEY] as MutableList<Double>).add(result.first)
                    (localData[RUNTIME_WITH_LOAD_KEY] as MutableList<Double>).add(result.second)
                    (localData[DCG_KEY] as MutableList<Double>).add(Measures.ndcg(gt.third, result.third))
                    (localData[RECALL_KEY] as MutableList<Double>).add(Measures.recall(gt.third, result.third))
                    (localData[RESULTS_KEY] as MutableList<List<Long>>).add(result.third)
                    (localData[GROUNDTRUTH_KEY] as MutableList<List<Long>>).add(result.third)
                }
            }

            /* Append data to map. */
            (this.data as MutableMap)[entity] = localData
        } finally { }
    }

    /**
     * Loads data collection
     */
    private fun loadCollection(entity: String): Pair<Boolean, Double> {
        /* Load collection. This is measured separately. */
        var loaded = false
        val loadDuration = measureTimeMillis {
            this.client.loadCollection(
                LoadCollectionParam.newBuilder().withCollectionName(entity).withSyncLoadWaitingTimeout(Constant.MAX_WAITING_LOADING_TIMEOUT).withSyncLoad(true).withSyncLoadWaitingInterval(1000).withSyncLoad(true).build()
            )
            val show = this.client.showCollections(ShowCollectionsParam.newBuilder().addCollectionName(entity).build())
            if (show.data.inMemoryPercentagesList.first() == 100L) {
                loaded = true
            }
        }

        /* If request times out then fallback. */
        return loaded to loadDuration / 1000.0
    }


    /**
     * Executes a query.
     */
    private fun executeQuery(entity: String, feature: FloatArray, k: Int = 10): Triple<Double, Double, List<Long>> {
        try {
            val loaded = this.loadCollection(entity)
            if (loaded.first) {
                val searchParam = SearchParam.newBuilder()
                    .withCollectionName(entity)
                    .withMetricType(MetricType.L2)
                    .withOutFields(listOf("id"))
                    .withTopK(k)
                    .withVectors(listOf(feature.toList()))
                    .withVectorFieldName("feature")
                    .build()


                /* Execute query and prepare results. */
                val results = ArrayList<Long>(k)
                val duration = measureTimeMillis {
                    val respSearch: R<SearchResults> = this.client.search(searchParam)
                    val wrapperSearch = SearchResultsWrapper(respSearch.data.results)
                    val ids = wrapperSearch.getFieldData("id",0)
                    ids.forEach { id -> results.add(id as Long) }
                }
                return Triple(duration / 1000.0, (duration / 1000.0) + loaded.second, results)
            } else {
                return Triple(Double.MAX_VALUE, Double.MAX_VALUE, emptyList())
            }
        } finally {
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName(entity).build()) /* Always release collection after executing a query. */
        }
    }
}
