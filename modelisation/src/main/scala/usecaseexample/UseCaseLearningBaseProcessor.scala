package usecaseexample

import DomainTables.CardUsageRefTable
import generators.BaseDateFeaturesGenerator
import org.apache.spark.sql.DataFrame


trait ChurnProcessing {

  def createPivotTable(df:DataFrame, pivotColumn:String):DataFrame
  def joinLearningTargetTable(strcutCarddf: DataFrame, JoinColCarddf: String,
                              targetdf: DataFrame, JoinColTargetdf: String):DataFrame
  def generateLearningFeatures(df:DataFrame, featureType:String):DataFrame
  def generateInferenceFeatures(df:DataFrame, featureType:String):DataFrame

}

object ChurnConfig {

  case class ChurnConfig(
                          segment:String,
                          featuresMonthMilestones:Seq[Int],
                          featuresMonthIntervals:Seq[(Int,Int, Int,Int)]
                        )

  val config = ChurnConfig(
    segment="segment1",
    featuresMonthMilestones=Seq(1,3,6),
    featuresMonthIntervals=Seq((0,1, 1,3), (1,3, 3,6))
  )

}

