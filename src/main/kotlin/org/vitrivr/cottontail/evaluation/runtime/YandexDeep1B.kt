package org.vitrivr.cottontail.evaluation.runtime

import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.evaluation.client
import org.vitrivr.cottontail.evaluation.datasets.iterators.YandexDeep1BIterator
import org.vitrivr.cottontail.evaluation.workingdir
import kotlin.system.measureTimeMillis


/**
 * Prepares the Yandex DEEP 1B in Cottontail DB.
 */
fun runYandexDeep1BSingle(k: Int = 10, warmup: Int = 1, iterations: Int = 10, into: Map<String,MutableList<*>>) {
    val bar = ProgressBarBuilder().setInitialMax((warmup + iterations).toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Running Benchmark (Yandex DEEP 1B, brute-force, single-threaded)").build()
    YandexDeep1BIterator(workingdir.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use {
        /* Warmup  queries. */
        bar.setExtraMessage("Warmup...")
        for (w in 0 until warmup) {
            val (_, feature) = it.next()
            warmup(k, feature)
            bar.step()
        }

        /* Benchmark queries. */
        bar.setExtraMessage("Benchmark...")
        for (w in 0 until iterations) {
            /* Indicate progress. */
            bar.step()

            val (_, feature) = it.next()
            val durationMs = measureTimeMillis {
                val query = Query().from(Query("evaluation.yandex_deep1b").limit(1_000_000))
                    .distance("feature", feature, Distances.L2, "distance")
                    .order("distance", Direction.ASC)
                    .limit(k.toLong())
                    .disallowParallelism()
                client.query(query).forEach { /* No op. */ }
            }

            /* Record data. */
            (into[CATEGORY_KEY] as MutableList<String>).add("Yandex Deep1B (brute-force, single-threaded)")
            (into[RUN_KEY] as MutableList<Int>).add(w + 1)
            (into[K_KEY] as MutableList<Int>).add(k)
            (into[RUNTIME_KEY] as MutableList<Double>).add(durationMs / 1000.0)

            /* Indicate progress. */
            bar.step()
        }
    }
}

/**
 * Prepares the Yandex DEEP 1B in Cottontail DB.
 */
fun runYandexDeep1BParallel(k: Int = 10, warmup: Int = 1, iterations: Int = 10, into: Map<String,MutableList<*>>) {
    val bar = ProgressBarBuilder().setInitialMax((warmup + iterations).toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Running Benchmark (Yandex DEEP 1B, Brute-force, multi-threaded)").build()
    YandexDeep1BIterator(workingdir.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use {
        /* Warmup  queries. */
        bar.setExtraMessage("Warmup...")
        for (w in 0 until warmup) {
            val (_, feature) = it.next()
            warmup(k, feature)
            bar.step()
        }

        /* Benchmark queries. */
        bar.setExtraMessage("Benchmark...")
        for (w in 0 until iterations) {
            /* Indicate progress. */
            bar.step()

            val (_, feature) = it.next()
            val durationMs = measureTimeMillis {
                val query = Query().from(Query("evaluation.yandex_deep1b").limit(1_000_000))
                    .distance("feature", feature, Distances.L2, "distance")
                    .order("distance", Direction.ASC)
                    .limit(k.toLong())
                client.query(query).forEach { /* No op. */ }
            }

            /* Record data. */
            (into[CATEGORY_KEY] as MutableList<String>).add("Yandex Deep1B (brute-force, parallel)")
            (into[RUN_KEY] as MutableList<Int>).add(w + 1)
            (into[K_KEY] as MutableList<Int>).add(k)
            (into[RUNTIME_KEY] as MutableList<Double>).add(durationMs / 1000.0)
        }
    }
}

/**
 * Executes a warmup query.
 */
private fun warmup(k: Int, feature: FloatArray) {
    val query = Query("evaluation.yandex_deep1b")
        .distance("feature", feature, Distances.L2, "distance")
        .order("distance", Direction.ASC)
        .limit(k.toLong())
    client.query(query).forEach { _ -> /* No op. */ }
}