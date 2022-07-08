package org.vitrivr.cottontail.evaluation

import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.evaluation.datasets.iterators.YandexDeep1BIterator
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files

import java.nio.file.StandardOpenOption


/**
 * Prepares the Yandex DEEP 1B in Cottontail DB.
 */
fun prepareDeep1B()  {
    /** Create entity for YANDEX Deep 1B dataset. */
    val create = CreateEntity("evaluation.yandex_deep1b")
        .column("id", Type.INTEGER, nullable = false)
        .column("feature", Type.FLOAT_VECTOR, 96, nullable =  false)
    client.create(create)

    /** Load YANDEX Deep 1B dataset. */
    YandexDeep1BIterator(workingdir.resolve("datasets/yandex-deep1b/base.1B.fbin")).use {
        val txId = client.begin()
        try {
            var insert = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature").txId(txId)
            while (it.hasNext()) {
                val next = it.next()
                if (!insert.append(next.first, next.second)) {
                    client.insert(insert)
                    insert = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature").txId(txId)
                    insert.append(next.first, next.second)
                }
            }

            /* Commit changes. */
            client.commit(txId)
        } catch (e:Throwable) {
            client.rollback(txId)
        }
    }
}



