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
import jetbrains.letsPlot.asDiscrete
import jetbrains.letsPlot.export.ggsave
import jetbrains.letsPlot.geom.geomBoxplot
import jetbrains.letsPlot.ggsize
import jetbrains.letsPlot.label.labs
import jetbrains.letsPlot.letsPlot
import jetbrains.letsPlot.scale.ylim
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
        RUN_KEY to mutableListOf<Int>(),
        K_KEY to mutableListOf<Int>(),
        RUNTIME_KEY to mutableListOf<Double>(),
        RUNTIME_WITH_OFFSET_KEY to mutableListOf<Double>(),
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
                IllegalStateException("")
            }
            this.data = Files.newBufferedReader(out.resolve("data.json")).use { Gson().fromJson(it, Map::class.java) } as Map<String,MutableList<Float>>

            /* Generate plots. */
            this.plot(out)
        } else {

            /* Make sure that all collections have been released. */
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep5m").build())
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep10m").build())
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep100m").build())
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName("yandex_deep1b").build())

            /* Determine and generate output directory. */
            val out = this.output ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
            if (!Files.exists(out)) {
                Files.createDirectories(out)
            }

            /* Yandex Deep 5M Series. */
            this.runYandexDeep1B("yandex_deep5m", k = this.k, warmup = this.warmup, iterations = this.repeat)
            this.runYandexDeep1B("yandex_deep10m", k = this.k, warmup = this.warmup, iterations = this.repeat)
            this.runYandexDeep1B("yandex_deep100m", k = this.k, warmup = this.warmup, iterations = this.repeat)
            this.runYandexDeep1B("yandex_deep1B", k = this.k, warmup = this.warmup, iterations = this.repeat,)

            /* Export raw data. */
            this.export(out)

            /* Generate plots. */
            this.plot(out)
        }
    }

    override fun plot(out: Path) {
        /* Prepare plot. */
        var runtimePlot = letsPlot(data)
        runtimePlot += geomBoxplot(color = "#A5D7D2", fill = "#D2EBE9", showLegend = false) { x = asDiscrete(ENTITY_KEY); y = RUNTIME_KEY; }
        runtimePlot += geomBoxplot(color = "#D20537", fill = "#D2EBE9", showLegend = false) { x = asDiscrete(ENTITY_KEY); y = RUNTIME_WITH_OFFSET_KEY; }
        runtimePlot += labs(x = "", y = "Runtime [s]")
        runtimePlot += ylim(listOf(0.0, 150.0))

        /* Recall plot. */
        var qualityPlot = letsPlot(data)
        qualityPlot += geomBoxplot(color = "#A5D7D2") { x = asDiscrete(ENTITY_KEY); y = RECALL_KEY; }
        qualityPlot += labs(x = "", y = "Quality")
        qualityPlot += ylim(listOf(0.0, 1.0))
        qualityPlot += ggsize(1000, 500)

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
            /* Load collection. This is measured separately. */
            val loadDuration = measureTimeMillis {
                val status = this.client.loadCollection(
                    LoadCollectionParam.newBuilder().withCollectionName(entity).withSyncLoadWaitingTimeout(Constant.MAX_WAITING_LOADING_TIMEOUT).withSyncLoad(true).withSyncLoadWaitingInterval(1000).withSyncLoad(true).build()
                )
                if (status.exception != null) {
                    val start = System.currentTimeMillis()
                    var poll = this.client.showCollections(ShowCollectionsParam.newBuilder().addCollectionName(entity).build())
                    while (poll.exception != null && (System.currentTimeMillis() - start >= 600_000L)) { /* Give it another 10 minutes. */
                        Thread.sleep(250)
                        poll = this.client.showCollections(ShowCollectionsParam.newBuilder().addCollectionName(entity).build())
                    }

                    /* If request times out then fallback. */
                    if (poll.exception != null) {
                        (this.data[ENTITY_KEY] as MutableList<String>).add(entity)
                        (this.data[RUN_KEY] as MutableList<Int>).add(1)
                        (this.data[K_KEY] as MutableList<Int>).add(k)
                        (this.data[RUNTIME_KEY] as MutableList<Double>).add(0.0)
                        (this.data[RUNTIME_WITH_OFFSET_KEY] as MutableList<Double>).add(900_000 / 1000.0)
                        (this.data[DCG_KEY] as MutableList<Double>).add(0.0)
                        (this.data[RECALL_KEY] as MutableList<Double>).add(0.0)
                    }
                }
            }

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
                bar.setExtraMessage("Benchmark...")
                for (w in 0 until iterations) {
                    /* Indicate progress. */
                    bar.step()

                    val (_, feature) = it.next()
                    val gt = executeQuery(entity, feature, k)
                    val result = executeQuery(entity, feature, k)

                    /* Record data. */
                    (this.data[ENTITY_KEY] as MutableList<String>).add(entity)
                    (this.data[RUN_KEY] as MutableList<Int>).add(w + 1)
                    (this.data[K_KEY] as MutableList<Int>).add(k)
                    (this.data[RUNTIME_KEY] as MutableList<Double>).add(result.first / 1000.0)
                    (this.data[RUNTIME_WITH_OFFSET_KEY] as MutableList<Double>).add((result.first + loadDuration) / 1000.0)
                    (this.data[DCG_KEY] as MutableList<Double>).add(Measures.ndcg(gt.second, result.second))
                    (this.data[RECALL_KEY] as MutableList<Double>).add(Measures.recall(gt.second, result.second))
                }
            }
        } finally {
            this.client.releaseCollection(ReleaseCollectionParam.newBuilder().withCollectionName(entity).build()) /* Always release collection after benchmark. */
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
            .withParams("{\"nprobe\":256}")
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
