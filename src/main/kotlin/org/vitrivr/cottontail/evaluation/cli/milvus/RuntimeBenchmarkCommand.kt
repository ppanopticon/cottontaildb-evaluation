package org.vitrivr.cottontail.evaluation.cli.milvus

import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import io.milvus.client.MilvusServiceClient
import io.milvus.grpc.DescribeCollectionResponse
import io.milvus.grpc.GetCollectionStatisticsResponse
import io.milvus.grpc.SearchResults
import io.milvus.param.MetricType
import io.milvus.param.R
import io.milvus.param.collection.DescribeCollectionParam
import io.milvus.param.collection.GetCollectionStatisticsParam
import io.milvus.param.collection.LoadCollectionParam
import io.milvus.param.collection.ReleaseCollectionParam
import io.milvus.param.dml.SearchParam
import io.milvus.response.DescCollResponseWrapper
import io.milvus.response.GetCollStatResponseWrapper
import io.milvus.response.SearchResultsWrapper
import jetbrains.letsPlot.export.ggsave
import jetbrains.letsPlot.geom.geomBoxplot
import jetbrains.letsPlot.ggsize
import jetbrains.letsPlot.letsPlot
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.vitrivr.cottontail.evaluation.cli.cottontail.AbstractBenchmarkCommand
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
class RuntimeBenchmarkCommand(private val client: MilvusServiceClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "runtime", help = "Prepares and loads all data required for Cottontail DB benchmarks.")  {

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
            /* Export JSON data. */
            require(this.output != null) { "Missing parameter --output."}
            this.data = Files.newBufferedReader(this.output!!.resolve("data.json")).use { Gson().fromJson(it, Map::class.java) } as Map<String,MutableList<Float>>
        } else {

            /* Yandex Deep 5M Series. */
            this.client.loadCollection(LoadCollectionParam.newBuilder().withCollectionName("yandex_deep5m").build())
            try {
               this.runYandexDeep1B("yandex_deep5m", k = this.k, warmup = this.warmup, iterations = this.repeat)
            } finally {
              this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep5m").build())
            }

            /* Yandex Deep 10M Series. */
            this.client.loadCollection(LoadCollectionParam.newBuilder().withCollectionName("yandex_deep10m").build())
            try {
                this.runYandexDeep1B("yandex_deep10m", k = this.k, warmup = this.warmup, iterations = this.repeat)
            } finally {
                this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep10m").build())
            }

            /* Yandex Deep 100M Series. */
            this.client.loadCollection(LoadCollectionParam.newBuilder().withCollectionName("yandex_deep100m").build())
            try {
                this.runYandexDeep1B("yandex_deep100m", k = this.k, warmup = this.warmup, iterations = this.repeat)
            } finally {
                this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep100m").build())
            }

            /* Yandex Deep 1B Series. */
            /* this.client.loadCollection(LoadCollectionParam.newBuilder().withCollectionName("yandex_deep1B").build())
            try {
                this.runYandexDeep1B("yandex_deep1B", k = this.k, warmup = this.warmup, iterations = this.repeat)
            } finally {
                this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep1B").build())
            }*/

            /* Export raw data. */
            this.export()
        }

        /* Generate plots. */
        this.plot()
    }

    override fun plot() {
        /* Make sure out directory exists. */
        val out = this.output ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
        if (!Files.exists(out)) {
            Files.createDirectories(out)
        }

        /* Prepare plot. */
        var runtimePlot = letsPlot(data) { x = TYPE_KEY; y = RUNTIME_KEY }
        runtimePlot += geomBoxplot(color = "#A5D7D2") { fill = ENTITY_KEY;  }
        runtimePlot += ggsize(1000, 500)

        /* Recall plot. */
        var recallPlot = letsPlot(data) { x = TYPE_KEY; y = RECALL_KEY }
        recallPlot += geomBoxplot(color = "#A5D7D2") { fill = ENTITY_KEY }
        recallPlot += ggsize(1000, 500)

        /* Export plot as PNG. */
        ggsave(runtimePlot, filename = "runtime.png", path = out.toString())
        ggsave(recallPlot, filename = "recall.png", path = out.toString())
    }

    /**
     * Exports a data set.
     */
    override fun export() {
        /* Make sure out directory exists. */
        val out = this.output ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
        if (!Files.exists(out)) {
            Files.createDirectories(out)
        }

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
        val indexName = indexType ?: "No Index"
        val type = "$indexName, $parallel"
        val bar = ProgressBarBuilder().setInitialMax((warmup + iterations).toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Running Benchmark ($type)").build()
        YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use {
            /* Warmup query. */
            bar.extraMessage = "Warmup..."
            for (w in 0 until warmup) {
                bar.step()
                val (_, feature) = it.next()
                executeQuery(entity, feature, k)
            }

            // Benchmark query.
            bar.setExtraMessage("Benchmark...")
            for (w in 0 until iterations) {
                /* Indicate progress. */
                bar.step()

                val (_, feature) = it.next()
                val gt = executeQuery(entity, feature, k)
                val result = executeQuery(entity, feature, k)

                /* Record data. */
                (this.data[ENTITY_KEY] as MutableList<String>).add(entity)
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
    private fun executeQuery(entity: String, feature: FloatArray, k: Int = 10): Pair<Long,List<Long>> {
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

        return duration to results
    }
}
