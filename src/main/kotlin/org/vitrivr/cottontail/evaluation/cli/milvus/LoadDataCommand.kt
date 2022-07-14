package org.vitrivr.cottontail.evaluation.cli.milvus

import com.github.ajalt.clikt.core.CliktCommand
import io.milvus.client.MilvusServiceClient
import io.milvus.grpc.DataType
import io.milvus.param.collection.CreateCollectionParam
import io.milvus.param.collection.FieldType
import io.milvus.param.dml.InsertParam
import me.tongfei.progressbar.ProgressBarBuilder
import me.tongfei.progressbar.ProgressBarStyle
import org.vitrivr.cottontail.evaluation.datasets.YandexDeep1BIterator
import java.nio.file.Path
import java.util.*

/**
 * Command to load data required for Milvu related benchmarks.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LoadDataCommand(private val client: MilvusServiceClient, private val workingDirectory: Path): CliktCommand(name = "load", help = "Prepares and loads all data required for Milvus benchmarks.") {
    /**
     * Executes command.
     */
    override fun run() {
        this.prepareDeep1B()
    }

    /**
     * Prepares the Yandex DEEP 1B in Cottontail DB.
     */
    private fun prepareDeep1B()  {
        /** Create entity for YANDEX Deep 1B dataset. */
        client.createCollection(
            CreateCollectionParam.newBuilder().withCollectionName("yandex_deep1b")
                .addFieldType(FieldType.newBuilder().withName("id").withPrimaryKey(true).withDataType(DataType.Int32).build())
                .addFieldType(FieldType.newBuilder().withName("category").withDataType(DataType.Int32).build())
                .addFieldType(FieldType.newBuilder().withName("feature").withDataType(DataType.FloatVector).withDimension(96).build())
                .build()
        )

        client.createCollection(
            CreateCollectionParam.newBuilder().withCollectionName("yandex_deep100m")
                .addFieldType(FieldType.newBuilder().withName("id").withPrimaryKey(true).withDataType(DataType.Int32).build())
                .addFieldType(FieldType.newBuilder().withName("category").withDataType(DataType.Int32).build())
                .addFieldType(FieldType.newBuilder().withName("feature").withDataType(DataType.FloatVector).withDimension(96).build())
                .build()
        )

        client.createCollection(
            CreateCollectionParam.newBuilder().withCollectionName("yandex_deep10m")
                .addFieldType(FieldType.newBuilder().withName("id").withPrimaryKey(true).withDataType(DataType.Int32).build())
                .addFieldType(FieldType.newBuilder().withName("category").withDataType(DataType.Int32).build())
                .addFieldType(FieldType.newBuilder().withName("feature").withDataType(DataType.FloatVector).withDimension(96).build())
                .build()
        )

        client.createCollection(
            CreateCollectionParam.newBuilder().withCollectionName("yandex_deep5m")
                .addFieldType(FieldType.newBuilder().withName("id").withPrimaryKey(true).withDataType(DataType.Int32).build())
                .addFieldType(FieldType.newBuilder().withName("category").withDataType(DataType.Int32).build())
                .addFieldType(FieldType.newBuilder().withName("feature").withDataType(DataType.FloatVector).withDimension(96).build())
                .build()
        )

        /** Load YANDEX Deep 1B dataset. */
        val iterator = YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/base.1B.fbin"))
        val bar = ProgressBarBuilder().setInitialMax(iterator.size.toLong()).setStyle(ProgressBarStyle.ASCII).setTaskName("Loading (Yandex Deep1B)").build()
        iterator.use {
            bar.use { b ->
                val random = SplittableRandom()
                val idList = ArrayList<Int>(10000)
                val categoryList = ArrayList<Int>(10000)
                val featureList =  ArrayList<FloatArray>(10000)
                while (it.hasNext()) {
                    val (id, vector) = it.next()
                    val category = random.nextInt(0, 10)

                    idList.add(id)
                    categoryList.add(category)
                    featureList.add(vector)
                    if (id % 10000 == 0) {
                        client.insert(
                            InsertParam.newBuilder().withCollectionName("yandex_deep1b").withFields(
                                listOf(
                                    InsertParam.Field("id", DataType.Int32, idList),
                                    InsertParam.Field("category", DataType.Int32, categoryList),
                                    InsertParam.Field("feature", DataType.FloatVector, featureList)
                                )
                            ).build()
                        )

                        if (id <= 100_000_000) {
                            client.insert(
                                InsertParam.newBuilder().withCollectionName("yandex_deep100m").withFields(
                                    listOf(
                                        InsertParam.Field("id", DataType.Int32, idList),
                                        InsertParam.Field("category", DataType.Int32, categoryList),
                                        InsertParam.Field("feature", DataType.FloatVector, featureList)
                                    )
                                ).build()
                            )
                        }

                        if (id < 10_000_000) {
                            client.insert(
                                InsertParam.newBuilder().withCollectionName("yandex_deep10m").withFields(
                                    listOf(
                                        InsertParam.Field("id", DataType.Int32, idList),
                                        InsertParam.Field("category", DataType.Int32, categoryList),
                                        InsertParam.Field("feature", DataType.FloatVector, featureList)
                                    )
                                ).build()
                            )
                        }

                        if (id <= 5_000_000) {
                            client.insert(
                                InsertParam.newBuilder().withCollectionName("yandex_deep5m").withFields(
                                    listOf(
                                        InsertParam.Field("id", DataType.Int32, idList),
                                        InsertParam.Field("category", DataType.Int32, categoryList),
                                        InsertParam.Field("feature", DataType.FloatVector, featureList)
                                    )
                                ).build()
                            )
                        }

                        idList.clear()
                        categoryList.clear()
                        featureList.clear()
                    }


                    /* Make step. */
                    b.step()
                }
            }
        }
    }
}