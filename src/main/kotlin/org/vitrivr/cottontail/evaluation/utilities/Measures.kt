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
        var relevantAndRetrieved = 0.0
        for (it in 0 until k.coerceAtMost(groundtruth.size)) {
            if (groundtruth.contains(test[it])) {
                relevantAndRetrieved += 1.0
            }
        }
        return relevantAndRetrieved/groundtruth.size
    }

    /**
     * Calculates the  discounted, cummulative gain (DCG) between a groundtruth and a test collection.
     *
     * @param groundtruth The list of groundtruth data to compare to.
     * @param test The [List] of test data.
     */
    fun <T> dcg(groundtruth: List<T>, test: List<T>): Double {
        var dcg = 0.0
        for ((index, item) in groundtruth.withIndex()) {
            val indexInTest = test.indexOf(item)
            dcg += if(indexInTest > 0) {
                (index + 1.0)/ log2(indexInTest + 2.0)
            } else {
                (index + 1.0)/ log2(groundtruth.size + 2.0)
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
        var dcg = 0.0
        var idcg = 0.0
        for ((index, item) in groundtruth.withIndex()) {
            val indexInTest = test.indexOf(item)
            dcg += if(indexInTest > -1) {
                (index + 1.0) / log2(indexInTest + 2.0)
            } else {
                (index + 1.0) / log2(Int.MAX_VALUE + 2.0)
            }
            idcg += (index + 1.0) / log2(index + 2.0)
        }
        return dcg / idcg
    }
}