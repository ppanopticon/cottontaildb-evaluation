package org.vitrivr.cottontail.evaluation

import me.tongfei.progressbar.ProgressBar
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.evaluation.datasets.iterators.YandexDeep1BIterator


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
    val iterator = YandexDeep1BIterator(workingdir.resolve("datasets/yandex-deep1b/base.1B.fbin"))
    val bar = ProgressBarBuilder().setInitialMax(iterator.size.toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Loading (Yandex Deep1B)").build()
    iterator.use {
        bar.use { b ->
            var txId = client.begin()
            try {
                var insert = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature").txId(txId)
                while (it.hasNext()) {
                    val (id, vector) = it.next()
                    if (!insert.append(id, vector)) {
                        client.insert(insert)
                        insert = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature").txId(txId)
                        insert.append(id, vector)
                        b.stepTo(id.toLong())
                    }

                    /* Intermediate commit every 1 mio entries. */
                    if (id % 1_000_000 == 0) {
                        b.extraMessage = "Committing..."
                        client.commit(txId)
                        txId = client.begin()
                        b.extraMessage = null
                    }
                }

                /* Commit changes. */
                b.setExtraMessage("Committing...")
                client.commit(txId)
            } catch (e:Throwable) {
                client.rollback(txId)
            }
        }
    }
}



