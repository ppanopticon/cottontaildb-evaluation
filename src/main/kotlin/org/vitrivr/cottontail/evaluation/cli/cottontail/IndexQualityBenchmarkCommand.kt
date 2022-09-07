package org.vitrivr.cottontail.evaluation.cli.cottontail

import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.google.gson.GsonBuilder
import io.milvus.param.IndexType
import me.tongfei.progressbar.ProgressBar
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.evaluation.cli.AbstractBenchmarkCommand
import org.vitrivr.cottontail.evaluation.datasets.YandexDeep1BIterator
import org.vitrivr.cottontail.evaluation.utilities.Measures
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.math.log2
import kotlin.math.pow

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IndexQualityBenchmarkCommand(private val client: SimpleClient, workingDirectory: Path): AbstractBenchmarkCommand(workingDirectory, name = "index-quality", help = "Prepares and loads all data required for Cottontail DB benchmarks.")  {

    /** Data frame that holds the data. */
    private var data: MutableMap<String,List<*>> = mutableMapOf()

    /** Flag that can be used to specify an [IndexType]. */
    private val index: String by option("-i", "--index", help = "The index to test.").required()

    /** Progress bar used*/
    private var progress: ProgressBar? = null

    /**
     *
     */
    override fun run() {
        /* Execute benchmark unless plot flag has been set. */
        val out = this.name?.let { this.workingDirectory.resolve("out/${it}") } ?: this.workingDirectory.resolve("out/${System.currentTimeMillis()}")
        if (!Files.exists(out)) {
            Files.createDirectories(out)
        }

        /* Initialize progress bar. */
        this.progress = ProgressBarBuilder().setInitialMax(this.repeat.toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("ANNS Benchmark:").build()

        /* Clear local data. */
        this.data.clear()
        this.data["level"] = mutableListOf<Int>()
        this.data["recall"] = mutableListOf<Double>()
        this.data["ndcg"] = mutableListOf<Double>()

        /* Execute. */
        try {
            YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use { iterator ->
                repeat(this.repeat) {
                    val (_, feature) = iterator.next()
                    val gt = this.executeNNSQuery("yandex_deep5m", feature, 1000, 32, null)
                    val r = this.executeNNSQuery("yandex_deep5m", feature, 1000, 32, this.index)

                    for (i in 0 until log2(10000.0f).toInt()) {
                        val level = 2.0.pow(i).toInt()
                        (this.data["level"] as MutableList<Int>).add(level)
                        (this.data["recall"] as MutableList<Double>).add(Measures.recall(gt, r, level))
                        (this.data["ndcg"] as MutableList<Double>).add(Measures.ndcg(gt, r, level))
                    }

                    /* Indicate progress. */
                    this.progress?.step()
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
     * Executes an NNS query that als fetches the feature vector.
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
     * Exports a data set.
     */
    override fun export(out: Path) {
        /* Export JSON data. */
        Files.newBufferedWriter(out.resolve("data.json"), StandardOpenOption.CREATE_NEW).use {
            val gson = GsonBuilder().setPrettyPrinting().create()
            gson.toJson(this.data, Map::class.java, gson.newJsonWriter(it))
        }
    }

}