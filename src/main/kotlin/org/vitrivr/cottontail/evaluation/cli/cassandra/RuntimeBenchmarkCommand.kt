package org.vitrivr.cottontail.evaluation.cli.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import org.vitrivr.cottontail.evaluation.cli.AbstractBenchmarkCommand
import org.vitrivr.cottontail.evaluation.datasets.YandexDeep1BIterator
import java.nio.file.Path

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RuntimeBenchmarkCommand(private val session: CqlSession?, workingDir: Path): AbstractBenchmarkCommand(workingDir, name = "runtime", help = "Prepares and executes an index benchmark test.")  {


    companion object {
        val FUNCTION =
            "double sum = 0.0;" +
            "for (int i = 0; i < a.size(); i++) {" +
                "sum += Math.pow((a.get(i) - b.get(i)), 2);" +
            "}" +
            "return Math.sqrt(sum);"
    }

    init {



    }


    override fun export(out: Path) {
        TODO("Not yet implemented")
    }



    override fun run() {
        /* Prepare schema. */
        try {
            this.session!!.execute("CREATE KEYSPACE IF NOT EXISTS evaluation WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        } catch (e: NullPointerException) {
            System.err.println("Apache Cassandra has not been initialised.")
            return
        }
        this.session.execute("DROP TABLE IF EXISTS evaluation.yandex_deep1b;")
        this.session.execute("CREATE TABLE evaluation.yandex_deep1b (id int PRIMARY KEY, feature list<float>);")
        this.session.execute("CREATE OR REPLACE FUNCTION evaluation.euclidean (a list<float>, b list<float>) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS '$FUNCTION';")

        /* Insert data. */
        YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/base.1B.fbin")).use { iterator ->
            val prepared = this.session.prepare("INSERT INTO evaluation.yandex_deep1b (id, feature) VALUES (:id, :feature);")
            repeat(1_000) {
                val (id, feature) = iterator.next()
                val bound = prepared.bind().setInt("id", id).setList("feature", (feature.toList() as List<java.lang.Float>), java.lang.Float::class.java)
                this.session.execute(bound)
            }
        }

        /* Execute query. */
        YandexDeep1BIterator(this.workingDirectory.resolve("datasets/yandex-deep1b/query.public.10K.fbin")).use { iterator ->
            val prepared = this.session.prepare("SELECT id AS distance FROM evaluation.yandex_deep1b ORDER BY evaluation.euclidean(feature, :query) ASC LIMIT 1000;")
            repeat(10) {
                val (_, feature) = iterator.next()
                val bound = prepared.bind().setList("query", (feature.toList() as List<java.lang.Float>), java.lang.Float::class.java)
                val results =  this.session.execute(bound).forEach {
                    println(it)
                }
            }
        }
    }
}