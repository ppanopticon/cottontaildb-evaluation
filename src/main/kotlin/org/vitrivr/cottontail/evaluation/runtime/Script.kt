package org.vitrivr.cottontail.evaluation.runtime

import jetbrains.letsPlot.export.ggsave
import jetbrains.letsPlot.geom.geomBoxplot
import jetbrains.letsPlot.ggsize
import jetbrains.letsPlot.letsPlot
import org.vitrivr.cottontail.evaluation.plotTheme
import org.vitrivr.cottontail.evaluation.workingdir

/**
 * Loads the datasets.
 */
fun runtime() {
    val data = mapOf<String,List<Float>> (
        "Yandex DEEP1B (brute-force, single-threaded)" to runYandexDeep1BSingle(warmup = 1, iterations = 5)
    )
    plotRuntime("test", data)
}


fun plotRuntime(name: String, data: Map<String, List<Float>>) {
    /* Prepare data. */
    val categories = mutableListOf<String>()
    val runtime = mutableListOf<Double>()
    for ((k,values) in data.entries) {
        for (v in values) {
            categories.add(k)
            runtime.add(v.toDouble())
        }
    }

    /* Prepare plot. */
    var p = letsPlot(mapOf("categories" to categories, "runtime" to runtime)) { x = "categories"; y = "runtime" }
    p += plotTheme
    p += geomBoxplot(color = "#A5D7D2") { fill = "categories" }
    p += ggsize(1000, 500)

    /* Export plot as PNG. */
    ggsave(p, filename = "${name}.png", path = workingdir.resolve("out").toString())
}