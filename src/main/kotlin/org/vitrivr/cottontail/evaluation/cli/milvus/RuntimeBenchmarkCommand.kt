package org.vitrivr.cottontail.evaluation.cli.milvus

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import io.milvus.client.MilvusServiceClient
import io.milvus.grpc.QueryResults
import io.milvus.grpc.SearchResults
import io.milvus.param.Constant
import io.milvus.param.IndexType
import io.milvus.param.MetricType
import io.milvus.param.R
import io.milvus.param.collection.LoadCollectionParam
import io.milvus.param.collection.ReleaseCollectionParam
import io.milvus.param.collection.ShowCollectionsParam
import io.milvus.param.dml.QueryParam
import io.milvus.param.dml.SearchParam
import io.milvus.param.index.CreateIndexParam
import io.milvus.param.index.DropIndexParam
import io.milvus.response.QueryResultsWrapper
import io.milvus.response.SearchResultsWrapper
import jetbrains.letsPlot.*
import jetbrains.letsPlot.export.ggsave
import jetbrains.letsPlot.facet.facetGrid
import jetbrains.letsPlot.geom.geomBar
import jetbrains.letsPlot.geom.geomErrorBar
import jetbrains.letsPlot.geom.geomPoint
import jetbrains.letsPlot.geom.geomText
import jetbrains.letsPlot.label.labs
import jetbrains.letsPlot.scale.scaleXDiscrete
import jetbrains.letsPlot.scale.scaleYContinuous
import jetbrains.letsPlot.scale.ylim
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.vitrivr.cottontail.evaluation.cli.cottontail.AbstractBenchmarkCommand
import org.vitrivr.cottontail.evaluation.constants.SCALE_COLORS
import org.vitrivr.cottontail.evaluation.constants.SCALE_FILLS
import org.vitrivr.cottontail.evaluation.constants.THEME
import org.vitrivr.cottontail.evaluation.datasets.YandexDeep1BIterator
import org.vitrivr.cottontail.evaluation.utilities.Measures
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.SplittableRandom
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
        private const val TYPE_KEY = "type"
    }

    /** Data frame that holds the data. */
    private var data: Map<String,Map<String,List<*>>> = HashMap()

    /** A [SplittableRandom] to generate categories. */
    private val random = SplittableRandom()

    /** Flag that can be used to specify an [IndexType]. */
    private val index: IndexType? by option("-t", "--type", help = "If set, then only the output will be plot.").convert { IndexType.valueOf(it.uppercase()) }

    /** Flag that can be used to provide `nprobe` configuration for [IndexType.IVF_PQ] and [IndexType.IVF_SQ8] indexes. */
    private val nprobe: String by option("-np", "--nprobe", help = "If set, then only the output will be plot.").convert { "{\"nprobe\":${it}}" }.default("")

    /** K is limited to 1000 entries. */
    private val k = 1000

    /** The list of entities to query.*/
    private val entities = listOf("yandex_deep5m", "yandex_deep10m", "yandex_deep100m" /*, "yandex_deep100m"*/)

    /**
     * Executes the command.
     */
    override fun run() {
        /* Execute benchmark unless plot flag has been set. */
        if (this.plotOnly) {
            /* Determine and generate output directory. */
            val measurement = this.name?.let { this.workingDirectory.resolve("out/${it}") } ?: Files.walk(this.workingDirectory.resolve("out")).filter { Files.isDirectory(it) }.sorted(Comparator.reverseOrder()).findFirst().orElseThrow {
                IllegalStateException("Could not identify a most recent output directory. Please specify output directory to plot.")
            }
            this.data = Files.newBufferedReader(measurement.resolve("data.json")).use { Gson().fromJson(it, Map::class.java) } as MutableMap<String,Map<String,List<*>>>

            /* Generate plots. */
            this.plot(measurement)
        } else {
            /* Make sure that all collections have been released and create / drop indexes. */
            for (e in this.entities) {
                this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName(e).build())
                if (this.index != null) {
                    val message = CreateIndexParam.newBuilder().withCollectionName(e).withFieldName("feature").withIndexType(this.index!!)
                        .withMetricType(MetricType.L2)
                        .withSyncMode(true)
                        .withSyncWaitingInterval(2000L)
                        .withSyncWaitingTimeout(14400)
                    if (this.index in setOf(IndexType.IVF_SQ8, IndexType.IVF_PQ))  message.withExtraParam("{\"nlist\":4096}")
                    this.client.createIndex(message.build())
                } else {
                    this.client.dropIndex(DropIndexParam.newBuilder().withCollectionName(e).withFieldName("feature").build())
                }
            }

            /* Determine and generate output directory. */
            val out = this.name?.let { this.workingDirectory.resolve("out/${it}") } ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
            if (!Files.exists(out)) {
                Files.createDirectories(out)
            }

            /* Run workloads. */
            for (e in this.entities) {
                this.runYandexDeep1B(e, warmup = this.warmup, iterations = this.repeat)
            }

            /* Export raw data. */
            this.export(out)

            /* Generate plots. */
            this.plot(out)
        }
    }

    override fun plot(out: Path) {
        /* Prepare runtime data. */
        val runtime = mapOf("x" to mutableListOf<String>(),
            "y" to mutableListOf<Double>(),
            "color" to mutableListOf<String>(),
            "facet" to mutableListOf<String>(),
            "max" to mutableListOf<String>(),
            "min" to mutableListOf<String>()
        )
        this.data.entries.sortedBy { it.key }.forEach { (entity ,v) ->
            (v[TYPE_KEY] as List<String>).distinct().forEach { type ->
                val stats1 = DescriptiveStatistics((v[TYPE_KEY] as List<String>).mapIndexedNotNull { index, t2 ->
                    if (type == t2) { (v[RUNTIME_KEY] as List<Double>)[index] } else { null }
                }.toDoubleArray())
                val stats2 =  DescriptiveStatistics((v[TYPE_KEY] as List<String>).mapIndexedNotNull { index, t2 ->
                    if (type == t2) { (v[RUNTIME_WITH_LOAD_KEY] as List<Double>)[index] } else { null }
                }.toDoubleArray())
                (runtime["x"] as MutableList<String>).addAll(listOf(type, type))
                (runtime["y"] as MutableList<Double>).addAll(listOf(stats1.mean, stats2.mean))
                (runtime["color"] as MutableList<String>).addAll(listOf("Query Only", "Query + Loading"))
                (runtime["facet"] as MutableList<String>).addAll(listOf(entity.replace("yandex_deep","").uppercase(), entity.replace("yandex_deep","").uppercase()))
                (runtime["max"] as MutableList<Double>).addAll(listOf(stats1.mean + stats1.standardDeviation, stats2.mean + stats2.standardDeviation))
                (runtime["min"] as MutableList<Double>).addAll(listOf(stats1.mean - stats1.standardDeviation, stats2.mean - stats2.standardDeviation))
            }
        }


        /* Prepare quality data. */
        val quality = mapOf("x" to mutableListOf<String>(),
            "y" to mutableListOf<Double>(),
            "facet" to mutableListOf<String>(),
            "max" to mutableListOf<String>(),
            "min" to mutableListOf<String>(),
        )
        this.data.entries.sortedBy { it.key }.forEach { (entity ,v) ->
            val stats1 = DescriptiveStatistics((v[DCG_KEY] as List<Double>).toDoubleArray())
            val stats2 =  DescriptiveStatistics((v[RECALL_KEY] as List<Double>).toDoubleArray())
            (quality["x"] as MutableList<String>).addAll(listOf("Recall", "DCG"))
            (quality["y"] as MutableList<Double>).addAll(listOf(stats1.mean, stats2.mean))
            (quality["facet"] as MutableList<String>).addAll(listOf(entity.replace("yandex_deep","").uppercase(), entity.replace("yandex_deep","").uppercase()))
            (quality["max"] as MutableList<Double>).addAll(listOf(stats1.mean + stats1.standardDeviation, stats2.mean + stats2.standardDeviation))
            (quality["min"] as MutableList<Double>).addAll(listOf(stats1.mean - stats1.standardDeviation, stats2.mean - stats2.standardDeviation))
        }

        /* Prepare plot. */
        var runtimePlot = letsPlot(runtime) { x = "x"; y = "y"; color = "color"; fill = "color"; }
        runtimePlot += geomBar(stat = Stat.identity, position = positionDodge(0.9)) {  }
        runtimePlot += geomErrorBar(width = 0.1, position = positionDodge(0.9), showLegend = false) { ymax = "max"; ymin = "min"; }
        runtimePlot += geomText(size = 6, angle = 90, position = positionDodge(0.9), hjust = "left", showLegend = false, labelFormat = ".2f") { label = "y"; y = "max"; }
        runtimePlot += scaleXDiscrete(name = "Workload")
        runtimePlot += scaleYContinuous(name = "Runtime [s]", limits = 0.0 to 220)
        runtimePlot += facetGrid("facet", xOrder = -1)
        runtimePlot += SCALE_COLORS
        runtimePlot += SCALE_FILLS
        runtimePlot += THEME.legendPosition(0.1, 1).legendJustification(0, 1.0).legendDirectionHorizontal()
        runtimePlot += ggsize(1000, 600)

        /* Recall plot. */
        var qualityPlot = letsPlot(quality) { x = "x"; y = "y"; }
        qualityPlot += geomPoint(position = Pos.dodge, stat = Stat.identity, size = 8, color = "#A5D7D2", fill = "#D20F37")
        qualityPlot += geomText(size = 6, angle = 315, position = positionNudge(0.0, -0.075), showLegend = false, labelFormat = ".2f") { label = "y"; }
        qualityPlot += labs(x = "Metric", y = "Quality")
        qualityPlot += ylim(listOf(0.0, 1.0))
        qualityPlot += facetGrid("facet", xOrder = -1)
        qualityPlot += THEME.legendPosition(1, 0.1).legendJustification(1, 1).legendDirectionHorizontal()
        qualityPlot += ggsize(1000, 600)

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
    private fun runYandexDeep1B(entity: String, warmup: Int = 1, iterations: Int = 10) {
        /* The local data. */
        val localData = mapOf<String,List<*>>(
            RUN_KEY to mutableListOf<Int>(),
            K_KEY to mutableListOf<Int>(),
            RUNTIME_KEY to mutableListOf<Double>(),
            RUNTIME_WITH_LOAD_KEY to mutableListOf<Double>(),
            DCG_KEY to mutableListOf<Double>(),
            RECALL_KEY to mutableListOf<Double>(),
            TYPE_KEY to mutableListOf<List<String>>(),
            RESULTS_KEY to mutableListOf<List<Long>>(),
            GROUNDTRUTH_KEY to mutableListOf<List<Long>>()
        )

        /* Now start benchmark. */
        val bar = ProgressBarBuilder().setInitialMax((warmup + iterations).toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Running Milvus Benchmark...").build()
        val gtMap = Files.newBufferedReader(this.workingDirectory.resolve("datasets/yandex-deep1b/groundtruth@1000.json")).use { Gson().fromJson(it, Map::class.java) } as Map<String,List<List<String>>>
        YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use { queryIterator ->
            /* Warmup query. */
            bar.extraMessage = "Warmup..."
            for (w in 0 until warmup) {
                bar.step()
                val (_, feature) = queryIterator.next()
                executeWorkload(entity, feature)
            }

            /* Start benchmark. */
            bar.extraMessage = "Benchmark..."
            for (w in 0 until iterations) {
                /* Indicate progress. */
                bar.step()

                /* Read groundtruth data (always 100). */
                val gt = arrayOf(
                    gtMap[entity]?.get(w * 3)?.map { it.toLong() } ?: emptyList(),
                    gtMap[entity]?.get(w * 3 + 1)?.map { it.toLong() } ?: emptyList(),
                    gtMap[entity]?.get(w * 3 + 2)?.map { it.toLong() } ?: emptyList()
                )

                /* Execute query. */
                val (_, feature) = queryIterator.next()
                val results = executeWorkload(entity, feature)

                /* Record data. */
                for (i in 0 until 3) {
                    (localData[RUN_KEY] as MutableList<Int>).add(w + 1)
                    (localData[K_KEY] as MutableList<Int>).add(k)
                    (localData[RESULTS_KEY] as MutableList<List<Long>>).add(results[i].third)
                    (localData[GROUNDTRUTH_KEY] as MutableList<List<Long>>).add(gt[i])
                    (localData[RUNTIME_KEY] as MutableList<Double>).add(results[i].first)
                    (localData[RUNTIME_WITH_LOAD_KEY] as MutableList<Double>).add(results[i].second)

                    if (gt.isEmpty()) {
                        (localData[DCG_KEY] as MutableList<Double>).add(Measures.ndcg(gt[i], results[i].third))
                        (localData[RECALL_KEY] as MutableList<Double>).add(Measures.recall(gt[i], results[i].third))
                    } else {
                        /* If there is no groundtruth, then we're building it. */
                        (localData[DCG_KEY] as MutableList<Double>).add(1.0)
                        (localData[RECALL_KEY] as MutableList<Double>).add(1.0)
                    }
                    when (i) {
                        0 -> (localData[TYPE_KEY] as MutableList<String>).add("NNS")
                        1 -> (localData[TYPE_KEY] as MutableList<String>).add("NNS + Fetch")
                        2 -> (localData[TYPE_KEY] as MutableList<String>).add("Hybrid")
                        else -> throw IllegalStateException("This should not happen!")
                    }
                }
            }

            /* Append data to map. */
            (this.data as MutableMap)[entity] = localData
        }
    }

    /**
     * Loads data collection. Waits for up to 10 minutes for collection to become available.
     *
     * @param entity The name of the collection to load.
     */
    private fun loadCollection(entity: String): Boolean {
        /* Load collection. This is measured separately. */
        var loaded: Boolean
        this.client.loadCollection(
            LoadCollectionParam.newBuilder().withCollectionName(entity).withSyncLoadWaitingTimeout(Constant.MAX_WAITING_LOADING_TIMEOUT).withSyncLoad(true).withSyncLoadWaitingInterval(1000).withSyncLoad(true).build()
        )
        val start = System.currentTimeMillis()
        do {
            val show = this.client.showCollections(ShowCollectionsParam.newBuilder().addCollectionName(entity).build())
            loaded = show.data.inMemoryPercentagesList.first() == 100L
        } while (!loaded && System.currentTimeMillis() - start <= 700_000)

        /* If request times out then fallback. */
        return loaded
    }

    /**
     * Executes a workload. This is the equivalent of executing a single round of queries.
     *
     * @param entity Name of the entity to search.
     * @param feature The query vector [FloatArray].
     */
    private fun executeWorkload(entity: String, feature: FloatArray): List<Triple<Double, Double, List<Long>>> {
        try {
            /* Time used for data loading. This is only done once per round. */
            val loaded: Boolean
            val durationLoading = measureTimeMillis {
                loaded = this.loadCollection(entity)
            }

            /* Now execute workload. */
            if (loaded) {
                val result1: List<Long>
                val time1 = measureTimeMillis {
                    result1 = this.executeNNSQuery(entity, feature)
                }

                val result2: List<Long>
                val time2 = measureTimeMillis {
                    result2 = this.executeNNSQueryWithFeature(entity, feature)
                }

                val result3: List<Long>
                val time3 = measureTimeMillis {
                    result3 = this.executeHybridQuery(entity, feature)
                }

                return listOf(
                    Triple(time1 / 1000.0, (time1 / 1000.0) + (durationLoading / 1000.0), result1),
                    Triple(time2 / 1000.0, (time2 / 1000.0) + (durationLoading / 1000.0), result2),
                    Triple(time3 / 1000.0, (time3 / 1000.0) + (durationLoading / 1000.0), result3)
                )
            } else {
                return listOf(
                    Triple(Double.MAX_VALUE, Double.MAX_VALUE, emptyList()),
                    Triple(Double.MAX_VALUE, Double.MAX_VALUE, emptyList()),
                    Triple(Double.MAX_VALUE, Double.MAX_VALUE, emptyList())
                )
            }
        } finally {
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName(entity).build()) /* Always release collection after executing a query. */
        }
    }

    /**
     * Executes a NNS search that only returns primary keys.
     *
     * @param entity Name of the entity to search.
     * @param feature The query vector [FloatArray].
     * @return List of primary keys.
     */
    private fun executeNNSQuery(entity: String, feature: FloatArray): List<Long> {
        val searchParam = SearchParam.newBuilder()
            .withCollectionName(entity)
            .withMetricType(MetricType.L2)
            .withOutFields(listOf("id"))
            .withTopK(k)
            .withVectors(listOf(feature.toList()))
            .withVectorFieldName("feature")
            .withParams(this.nprobe)
            .build()

        val search: R<SearchResults> = this.client.search(searchParam)
        val results = SearchResultsWrapper(search.data.results)
        return results.getFieldData("id",0) as List<Long>
    }

    /**
     * Executes a NNS search that only returns primary keys.
     *
     * @param entity Name of the entity to search.
     * @param feature The query vector [FloatArray].
     * @return List of primary keys.
     */
    private fun executeHybridQuery(entity: String, feature: FloatArray): List<Long> {
        val category = this.random.nextInt(0, 10)
        val searchParam = SearchParam.newBuilder()
            .withCollectionName(entity)
            .withMetricType(MetricType.L2)
            .withOutFields(listOf("id"))
            .withTopK(this.k)
            .withVectors(listOf(feature.toList()))
            .withVectorFieldName("feature")
            .withExpr("category == $category")
            .withParams(this.nprobe)
            .build()

        val search: R<SearchResults> = this.client.search(searchParam)
        val results = SearchResultsWrapper(search.data.results)
        return results.getFieldData("id",0) as List<Long>
    }

    /**
     * Executes a NNS search and fetches the associated feature vectors afterwards.
     *
     * @param entity Name of the entity to search.
     * @param feature The query vector [FloatArray].
     * @return List of primary keys.
     */
    private fun executeNNSQueryWithFeature(entity: String, feature: FloatArray): List<Long> {
        /* Prepare query. */
        val searchParam1 = SearchParam.newBuilder()
            .withCollectionName(entity)
            .withMetricType(MetricType.L2)
            .withOutFields(listOf("id"))
            .withTopK(k)
            .withVectors(listOf(feature.toList()))
            .withVectorFieldName("feature")
            .withParams(this.nprobe)
            .build()

        /* Execute NNS. */
        val search: R<SearchResults> = this.client.search(searchParam1)
        val searchResults = SearchResultsWrapper(search.data.results)
        val ids = searchResults.getFieldData("id",0)

        /* Fetch features. */
        val searchParam2 = QueryParam.newBuilder()
            .withCollectionName(entity)
            .withOutFields(listOf("feature"))
            .withExpr("id in ${ids.joinToString(",", "[", "]")}")
            .build()

        /* These are not used. */
        val query: R<QueryResults> = this.client.query(searchParam2)
        val queryResults = QueryResultsWrapper(query.data)
        queryResults.getFieldWrapper("feature").fieldData
        return ids as List<Long>
    }
}
