package org.vitrivr.cottontail.evaluation

import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files

import java.nio.file.StandardOpenOption

fun main() {
    /* Start with empty schema. */
    try {
        client.drop(DropSchema("evaluation"))
    } catch (e: StatusRuntimeException) {
        if (e.status.code == Status.Code.NOT_FOUND) {
            /* Ignore. */
        }
    } finally {
        client.create(CreateSchema("evaluation"))
    }

    /* Load datasets. */
    prepareDeep1B()
}

/**
 * Prepares the Yandex DEEP 1B in Cottontail DB.
 */
private fun prepareDeep1B()  {
    /** Create entity for YANDEX Deep 1B dataset. */
    val create = CreateEntity("evaluation.yandex_deep1b")
        .column("id", Type.INTEGER, nullable = false)
        .column("feature", Type.FLOAT_VECTOR, 96, nullable =  false)
    client.create(create)

    /** Load YANDEX Deep 1B dataset. */
    Files.newByteChannel(workingdir.resolve("datasets/yandex-deep1b/base.1B.fbin"), StandardOpenOption.READ).use { it ->
        val intBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        if (it.read(intBuffer) <= 0) return@use
        val size = intBuffer.flip().int
        if (it.read(intBuffer.flip()) <= 0) return@use
        val d = intBuffer.flip().int
        require(d == 96) { "Dimension mismatch; expected 96 but found $d." }

        /** Allocate data structures. */
        val vectorBuffer = ByteBuffer.allocate(d * 4).order(ByteOrder.LITTLE_ENDIAN)

        /** Read data and import it into Cottontail DB. */
        val txId = client.begin()
        try {
            var insert = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature").txId(txId)
            for (i in 0 until size) {
                if (it.read(vectorBuffer) < vectorBuffer.capacity()) throw IllegalStateException("Premature end of file detected")
                vectorBuffer.flip()
                val vector = FloatArray(d) { vectorBuffer.float }
                if (!insert.append(i + 1, vector)) {
                    client.insert(insert)
                    insert = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature").txId(txId)
                    insert.append(i + 1, vector)
                }

                /* Prepare buffer for next read. */
                vectorBuffer.flip()
            }

            /* Commit changes. */
            client.commit(txId)
        } catch (e:Throwable) {
            client.rollback(txId)
        }
    }
}



