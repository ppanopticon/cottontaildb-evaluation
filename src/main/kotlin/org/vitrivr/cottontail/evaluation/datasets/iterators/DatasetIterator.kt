package org.vitrivr.cottontail.evaluation.datasets.iterators

import java.io.Closeable

/**
 * A [Iterator] for datasets, usually consisting of a [Int] ID and a [FloatArray] data point.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface DatasetIterator: Iterator<Pair<Int,FloatArray>>, Closeable