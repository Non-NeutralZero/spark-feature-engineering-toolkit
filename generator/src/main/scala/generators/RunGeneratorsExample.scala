package generators

import generators.BaseDateFeaturesGenerator.{counts, valuetrends}
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import toolkits.DataLoading

object RunGeneratorsExample {

  def main(args: Array[String]): Unit = {

    val df = DataLoading.loadCsv("src/test/resources/data-engineering-dataset.csv")
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))

    BaseDateFeaturesGenerator.LearningFeaturesGenerator(
        df,
        "timestamp_interaction",
        "montant_dhs",
        "id_client",
        "pivot",
        "",
        Seq(1, 3, 6),
        Seq((0,1, 3,6)),
        Left("2019-07-01"),
        counts.featuretype
      )
      .na.fill(0)
      .show()

    BaseDateFeaturesGenerator.LearningFeaturesGenerator(
        df,
        "timestamp_interaction",
        "montant_dhs",
        "id_client",
        "pivot",
        "",
        Seq(1, 3, 6),
        Seq((0,1, 3,6)),
        Left("2019-07-01"),
        valuetrends.featuretype
      )
      .na.fill(0)
      .show()


  }

}
