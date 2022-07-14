package org.vitrivr.cottontail.evaluation.datasets

import java.io.Closeable

/**
 * A [Iterator] for datasets, usually consisting of a [Int] ID and a [FloatArray] data point.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface DatasetIterator: Iterator<Pair<Int,FloatArray>>, Closeable {
    /** The number of entries in the data set iterated by this [DatasetIterator].*/
    val size: Int

    /** The dimensionality of the vectors. */
    val dimension: Int
}