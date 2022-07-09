package org.vitrivr.cottontail.evaluation.runtime

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import jetbrains.letsPlot.export.ggsave
import jetbrains.letsPlot.geom.geomBoxplot
import jetbrains.letsPlot.ggsize
import jetbrains.letsPlot.letsPlot
import org.vitrivr.cottontail.evaluation.outdir
import org.vitrivr.cottontail.evaluation.plotTheme
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption


val CATEGORY_KEY = "category"
val RUN_KEY = "run"
val K_KEY = "k"
val RUNTIME_KEY = "runtime"
/**
 * Loads the datasets.
 */
fun runtime() {
    /* Prepare data frame. */
    val data = mapOf<String,MutableList<*>>(
        CATEGORY_KEY to mutableListOf<String>(),
        RUN_KEY to mutableListOf<Int>(),
        K_KEY to mutableListOf<Int>(),
        RUNTIME_KEY to mutableListOf<Double>()
    )

    /* Execute benchmark. */
    runYandexDeep1BSingle(warmup = 0, iterations = 5, into = data)
    runYandexDeep1BParallel(warmup = 0, iterations = 5, into = data)

    /* Export raw data. */
    export(data)

    /* Plot data. */
    plot("test", data)
}


/**
 * Exports a data set.
 */
fun export(data: Map<String, List<*>>) {
    /* Make sure out directory exists. */
    if (!Files.exists(outdir)) {
        Files.createDirectories(outdir)
    }

    /* Export JSON data. */
    Files.newBufferedWriter(outdir.resolve("data.json"), StandardOpenOption.CREATE_NEW).use {
        val gson = GsonBuilder().setPrettyPrinting().create()
        gson.toJson(data, Map::class.java, gson.newJsonWriter(it))
    }
}

fun plot(name: String, path: Path) {
    /* Export JSON data. */
    val data = Files.newBufferedReader(path).use {
        val gson = Gson()
        gson.fromJson(it, Map::class.java)
    } as Map<String,List<Float>>

    plot("test", data, path.parent)
}

fun plot(name: String, data: Map<String, List<*>>, path: Path = outdir) {
    /* Make sure out directory exists. */
    if (!Files.exists(outdir)) {
        Files.createDirectories(outdir)
    }

    /* Prepare plot. */
    var p = letsPlot(data) { x = CATEGORY_KEY; y = RUNTIME_KEY }
    p += plotTheme
    p += geomBoxplot(color = "#A5D7D2") { fill = CATEGORY_KEY }
    p += ggsize(1000, 500)

    /* Export plot as PNG. */
    ggsave(p, filename = "${name}.png", path = path.toString())
}