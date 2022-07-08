package org.vitrivr.cottontail.evaluation.datasets

import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.evaluation.client
import org.vitrivr.cottontail.evaluation.prepareDeep1B

/**
 * Loads the datasets.
 */
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
