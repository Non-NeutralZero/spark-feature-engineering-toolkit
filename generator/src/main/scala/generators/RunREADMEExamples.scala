package generators

import featuressuite.EventFeatureEngineeringImplicits.EventBasedFeatureEngineeringFunctions
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import toolkits.DataLoading

object RunREADMEExamples {

  def main(args: Array[String]): Unit = {


  val df = DataLoading.loadCsv("src/test/resources/data-engineering-dataset.csv")

  // First example in Readme
  df.withColumn("basedt", lit("2019-07-01"))
    .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
    .getCountEventsWithinMilestonesFeatures("timestamp_interaction", "basedt", "id_client", "pivot", "", Seq(1, 3, 6))
    .na.fill(0).show(truncate = false)

  // Second example in Readme
  df
    .withColumn("basedt", lit("2019-07-01"))
    .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
    .getCountsSumsValuesWithinMilestonesFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", "mt", Seq(1, 3, 6))
    .na.fill(0).show(truncate = false)

  // Third example in Readme
  df
    .withColumn("basedt", lit("2019-07-01"))
    .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
    .getValueTrendFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", Seq((1, 6, 6, 12)))
    .na.fill(0).show(truncate = false)
}

}
