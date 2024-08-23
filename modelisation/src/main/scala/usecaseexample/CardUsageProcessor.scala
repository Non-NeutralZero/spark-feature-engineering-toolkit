package usecaseexample

import generators.BaseDateFeaturesGenerator
import implicits.DataFrameStandardizingImplicits.DataFrameStandardizingFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, lit}


class CardUsageProcessor extends ChurnProcessing {

  def createPivotTable(df: DataFrame, pivotColumn: String): DataFrame =
    df
      .select(
        "id_client",
        "timestamp_interaction",
        "montant_dhs",
        "lieu",
        "type"
      )
      .withColumn("pivot", concat_ws("_", lit(pivotColumn), col(pivotColumn)))
      .cleanColumnValues(Seq("pivot"))

  def joinLearningTargetTable(strcutCarddf: DataFrame, JoinColCarddf: String,
                              targetdf: DataFrame, JoinColTargetdf: String): DataFrame =
    strcutCarddf
      .join(targetdf, col(JoinColCarddf) === targetdf(JoinColTargetdf))


  def generateLearningFeatures(df: DataFrame, featureType: String): DataFrame ={
    BaseDateFeaturesGenerator.LearningFeaturesGenerator(
        df,
        "timestamp_interaction",
        "montant_dhs",
        "id_client",
        "pivot",
        "",
        ChurnConfig.config.featuresMonthMilestones,
        ChurnConfig.config.featuresMonthIntervals,
        Right(col("churn_date")),
        featureType
      )
      .na.fill(0)
  }

  def generateInferenceFeatures(df: DataFrame, featureType: String): DataFrame ={
    BaseDateFeaturesGenerator.LearningFeaturesGenerator(
        df,
        "timestamp_interaction",
        "montant_dhs",
        "id_client",
        "pivot",
        "",
        ChurnConfig.config.featuresMonthMilestones,
        ChurnConfig.config.featuresMonthIntervals,
        Left("now"),
        featureType
      )
      .na.fill(0)
  }


}