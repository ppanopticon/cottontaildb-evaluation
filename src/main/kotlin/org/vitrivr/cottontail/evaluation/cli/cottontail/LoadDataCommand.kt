package org.vitrivr.cottontail.evaluation.cli.cottontail

import com.github.ajalt.clikt.core.CliktCommand
import io.grpc.Status
import io.grpc.StatusRuntimeException
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.evaluation.datasets.YandexDeep1BIterator
import java.nio.file.Path
import java.util.*

/**
 * Loads all data required to execute Cottontail DB related benchmarks.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LoadDataCommand(private val client: SimpleClient, private val workingDir: Path): CliktCommand(name = "load", help = "Prepares and loads all data required for Cottontail DB benchmarks.") {

    /**
     * Executes the command.
     */
    override fun run() {
        try {
            this.client.drop(DropSchema("evaluation"))
        } catch (e: StatusRuntimeException) {
            if (e.status.code == Status.Code.NOT_FOUND) {/* Ignore. */ }
        } finally {
            this.client.create(CreateSchema("evaluation"))
        }
    }

    /**
     * Loads and prepares the Yandex DEEP 1B in Cottontail DB.
     */
    private fun yandexDeep1B() {

        fun prepareDeep1B() {
            /** Create entity for YANDEX Deep 1B dataset. */
            val create1 = CreateEntity("evaluation.yandex_deep1b")
                .column("id", Type.INTEGER, nullable = false)
                .column("feature", Type.FLOAT_VECTOR, 96, nullable = false)
                .column("category", Type.INTEGER, nullable = false)

            val create2 = CreateEntity("evaluation.yandex_deep100m")
                .column("id", Type.INTEGER, nullable = false)
                .column("feature", Type.FLOAT_VECTOR, 96, nullable = false)
                .column("category", Type.INTEGER, nullable = false)

            val create3 = CreateEntity("evaluation.yandex_deep10m")
                .column("id", Type.INTEGER, nullable = false)
                .column("feature", Type.FLOAT_VECTOR, 96, nullable = false)
                .column("category", Type.INTEGER, nullable = false)

            val create4 = CreateEntity("evaluation.yandex_deep5m")
                .column("id", Type.INTEGER, nullable = false)
                .column("feature", Type.FLOAT_VECTOR, 96, nullable = false)
                .column("category", Type.INTEGER, nullable = false)

            this.client.create(create1)
            this.client.create(create2)
            this.client.create(create3)
            this.client.create(create4)

            /** Load YANDEX Deep 1B dataset. */
            val iterator = YandexDeep1BIterator(this.workingDir.resolve("datasets/yandex-deep1b/base.1B.fbin"))
            val bar = ProgressBarBuilder().setInitialMax(iterator.size.toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Loading (Yandex Deep1B)").build()
            iterator.use {
                bar.use { b ->
                    var txId = this.client.begin()
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
                                this.client.commit(txId)
                                txId = this.client.begin()
                                b.extraMessage = ""
                            }

                            if (!insert1.append(id, vector, category)) {
                                this.client.insert(insert1.txId(txId))
                                insert1 = BatchInsert("evaluation.yandex_deep1b").columns("id", "feature", "category")
                                insert1.append(id, vector, category)
                            }

                            if (id <= 100_000_000 && !insert2.append(id, vector, category)) {
                                this.client.insert(insert2.txId(txId))
                                insert2 = BatchInsert("evaluation.yandex_deep100m").columns("id", "feature", "category")
                                insert2.append(id, vector, category)
                            }

                            if (id <= 10_000_000 && !insert3.append(id, vector, category)) {
                                this.client.insert(insert3.txId(txId))
                                insert3 = BatchInsert("evaluation.yandex_deep10m").columns("id", "feature", "category")
                                insert3.append(id, vector, category)
                            }

                            if (id <= 5_000_000 && !insert4.append(id, vector, category)) {
                                this.client.insert(insert4.txId(txId))
                                insert4 = BatchInsert("evaluation.yandex_deep5m").columns("id", "feature", "category")
                                insert4.append(id, vector, category)
                            }

                            /* Make step. */
                            b.step()
                        }

                        /* Final insert. */
                        this.client.insert(insert1.txId(txId))
                        this.client.insert(insert2.txId(txId))
                        this.client.insert(insert3.txId(txId))
                        this.client.insert(insert4.txId(txId))

                        /* Commit changes. */
                        b.setExtraMessage("Committing...")
                        this.client.commit(txId)
                    } catch (e: Throwable) {
                        this.client.rollback(txId)
                    }
                }
            }
        }
    }
}