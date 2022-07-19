package org.vitrivr.cottontail.evaluation.utilities

import kotlin.math.log2

/**
 * A utility class to calculate different quality measures.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Measures {
    /**
     * Calculates the recall at a given recall level [k].
     *
     * @param groundtruth The list of groundtruth data to compare to.
     * @param test The [List] of test data.
     * @param k Optional recall level.
     */
    fun <T> recall(groundtruth: List<T>, test: List<T>, k: Int = groundtruth.size): Double {
        if (groundtruth.isEmpty()) return 0.0
        var relevantAndRetrieved = 0.0
        for (i in 0 until k.coerceAtMost(groundtruth.size)) {
            if (test.contains(groundtruth[i])) {
                relevantAndRetrieved += 1.0
            }
        }
        return relevantAndRetrieved / groundtruth.size
    }

    /**
     * Calculates the  discounted, cummulative gain (DCG) between a groundtruth and a test collection.
     *
     * @param groundtruth The list of groundtruth data to compare to.
     * @param test The [List] of test data.
     */
    fun <T> dcg(groundtruth: List<T>, test: List<T>): Double {
        if (groundtruth.isEmpty()) return 0.0
        var dcg = 0.0
        for ((index, item) in test.withIndex()) {
            val indexInGroundtruth = groundtruth.indexOf(item)
            if(indexInGroundtruth > -1) {
                dcg += (groundtruth.size + 1.0 - indexInGroundtruth) / log2(index + 2.0)
            }
        }
        return dcg
    }

    /**
     * Calculates the normalised, discounted, cummulative gain (nDCG) between a groundtruth and a test collection.
     *
     * @param groundtruth The list of groundtruth data to compare to.
     * @param test The [List] of test data.
     */
    fun <T> ndcg(groundtruth: List<T>, test: List<T>): Double {
        if (groundtruth.isEmpty()) return 0.0
        var dcg = 0.0
        var idcg = 0.0
        for ((index, item) in test.withIndex()) {
            val indexInGroundtruth = groundtruth.indexOf(item)
            if(indexInGroundtruth > -1) {
                dcg += (groundtruth.size + 1.0 - indexInGroundtruth) / log2(index + 2.0) /* Index is 0-based, i.e., + 2.0. */
            }
        }
        for (index in test.indices) {
            idcg += (groundtruth.size + 1.0 - index) / log2(index + 2.0) /* Index is 0-based, i.e., + 2.0. */
        }
        return dcg / idcg
    }
}