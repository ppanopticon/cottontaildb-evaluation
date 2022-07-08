package org.vitrivr.cottontail.evaluation.datasets.iterators

import org.vitrivr.cottontail.evaluation.workingdir
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.SeekableByteChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * A [DatasetIterator] to read Yandex Deep 1B data files.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class YandexDeep1BIterator(path: Path): DatasetIterator {
    /** The [SeekableByteChannel] used to access the .fbin file.*/
    private val channel: SeekableByteChannel = Files.newByteChannel(workingdir.resolve(path), StandardOpenOption.READ)

    /** The number of entries in the file wrapped by this [YandexDeep1BIterator].*/
    override val size: Int

    /** The number of entries in the file wrapped by this [YandexDeep1BIterator].*/
    override val dimension: Int

    /** The current position (in terms of entries) of this [YandexDeep1BIterator].e */
    private var position = 1

    /** Internal [ByteBuffer] for data. */
    private var vectorBuffer: ByteBuffer

    init {
        /** Load YANDEX Deep 1B dataset. */
        val intBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        if (channel.read(intBuffer) <= 0) throw IllegalStateException("Could not read the number of entries from .fbin file.")
        this.size = intBuffer.flip().int
        if (channel.read(intBuffer.flip()) <= 0) throw IllegalStateException("Could not read the number of dimensions from .fbin file.")
        this.dimension = intBuffer.flip().int
        require( this.dimension == 96) { "Dimension mismatch; expected 96 but found ${this.dimension}." }
        this.vectorBuffer = ByteBuffer.allocate(this.dimension * 4).order(ByteOrder.LITTLE_ENDIAN)
    }

    /**
     * Returns the next entry in this [YandexDeep1BIterator].
     *
     * @return [Pair] of [Int] to [FloatArray]
     */
    override fun hasNext(): Boolean = this.position < this.size

    /**
     * Returns the next entry in this [YandexDeep1BIterator].
     *
     * @return [Pair] of [Int] to [FloatArray]
     */
    override fun next(): Pair<Int, FloatArray> {
        if (this.channel.read(this.vectorBuffer.clear()) < this.vectorBuffer.capacity()) throw IllegalStateException("Premature end of file detected")
        this.vectorBuffer.flip()
        val ret = FloatArray(this.dimension) { vectorBuffer.float }
        vectorBuffer.flip()
        return (this.position++ to ret)
    }

    /**
     * Closes this [YandexDeep1BIterator].
     */
    override fun close() {
        if (this.channel.isOpen) {
            this.channel.close()
        }
    }
}