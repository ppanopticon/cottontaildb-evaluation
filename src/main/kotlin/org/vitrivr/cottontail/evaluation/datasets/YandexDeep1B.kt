package org.vitrivr.cottontail.evaluation

import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.evaluation.datasets.iterators.YandexDeep1BIterator
import java.util.SplittableRandom

/**
 * Prepares the Yandex DEEP 1B in Cottontail DB.
 */
fun prepareDeep1B()  {
    /** Create entity for YANDEX Deep 1B dataset. */
    val create1 = CreateEntity("evaluation.yandex_deep1b")
        .column("id", Type.INTEGER, nullable = false)
        .column("feature", Type.FLOAT_VECTOR, 96, nullable =  false)
        .column("category", Type.INTEGER, nullable =  false)

    val create2 = CreateEntity("evaluation.yandex_deep100m")
        .column("id", Type.INTEGER, nullable = false)
        .column("feature", Type.FLOAT_VECTOR, 96, nullable =  false)
        .column("category", Type.INTEGER, nullable =  false)

    val create3 = CreateEntity("evaluation.yandex_deep10m")
        .column("id", Type.INTEGER, nullable = false)
        .column("feature", Type.FLOAT_VECTOR, 96, nullable =  false)
        .column("category", Type.INTEGER, nullable =  false)

    val create4 = CreateEntity("evaluation.yandex_deep5m")
        .column("id", Type.INTEGER, nullable = false)
        .column("feature", Type.FLOAT_VECTOR, 96, nullable =  false)
        .column("category", Type.INTEGER, nullable =  false)

    client.create(create1)
    client.create(create2)
    client.create(create3)
    client.create(create4)

    /** Load YANDEX Deep 1B dataset. */
    val iterator = YandexDeep1BIterator(workingdir.resolve("datasets/yandex-deep1b/base.1B.fbin"))
    val bar = ProgressBarBuilder().setInitialMax(iterator.size.toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Loading (Yandex Deep1B)").build()
    iterator.use {
        bar.use { b ->
            var txId = client.begin()
            try {
                var insert1 = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature", "category")
                var insert2 = BatchInsert("evaluation.yandex_deep100m").columns("id", "feature", "category")
                var insert3 = BatchInsert("evaluation.yandex_deep10m").columns("id", "feature", "category")
                var insert4 = BatchInsert("evaluation.yandex_deep5m").columns("id", "feature", "category")

                val random = SplittableRandom()
                while (it.hasNext()) {
                    val (id, vector) = it.next()
                    val category = random.nextInt(0, 10)

                    /* Intermediate commit every 1 mio entries. */
                    if (id % 1_000_000 == 0) {
                        b.extraMessage = "Committing..."
                        client.commit(txId)
                        txId = client.begin()
                        b.extraMessage = ""
                    }

                    if (!insert1.append(id, vector, category)) {
                        client.insert(insert1.txId(txId))
                        insert1 = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature", "category")
                        insert1.append(id, vector, category)
                    }

                    if (id <= 100_000_000 && !insert2.append(id, vector, category)) {
                        client.insert(insert1.txId(txId))
                        insert2 = BatchInsert("evaluation.yandex_deep100m").columns("id", "feature", "category")
                        insert2.append(id, vector, category)
                    }

                    if (id <= 10_000_000 && !insert3.append(id, vector, category)) {
                        client.insert(insert3.txId(txId))
                        insert3 = BatchInsert("evaluation.yandex_deep10m").columns("id", "feature", "category")
                        insert3.append(id, vector, category)
                    }

                    if (id <= 5_000_000 && !insert4.append(id, vector, category)) {
                        client.insert(insert4.txId(txId))
                        insert4 = BatchInsert("evaluation.yandex_deep5m").columns("id", "feature", "category")
                        insert4.append(id, vector, category)
                    }

                    /* Make step. */
                    b.step()
                }

                /* Final insert. */
                client.insert(insert1.txId(txId))
                client.insert(insert2.txId(txId))
                client.insert(insert3.txId(txId))
                client.insert(insert4.txId(txId))

                /* Commit changes. */
                b.setExtraMessage("Committing...")
                client.commit(txId)
            } catch (e:Throwable) {
                client.rollback(txId)
            }
        }
    }
}



