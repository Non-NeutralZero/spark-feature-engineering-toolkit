package generators

import org.apache.spark.sql.{Column, DataFrame}
import featuressuite.EventFeatureEngineeringImplicits.EventBasedFeatureEngineeringFunctions
import implicits.DataFrameStandardizingImplicits.DataFrameStandardizingFunctions
import org.apache.spark.sql.functions.{col, lit}
import toolkits.TimeToolkit.nowDate

object BaseDateFeaturesGenerator {

  case class FeaturesType(featuretype:String)

  val counts : FeaturesType = FeaturesType("counts")
  val countsplussums : FeaturesType = FeaturesType("countsplussums")
  val valuetrends : FeaturesType = FeaturesType("valuetrends")


  def LearningFeaturesGenerator(baseTable: DataFrame,
                                dateCol:String,
                                valCol:String,
                                grpByCol:String,
                                pivotCol:String,
                                lib:String="",
                                milestones:Seq[Int],
                                intervals:Seq[(Int, Int, Int, Int)],
                                baseDateSource: Either[String,Column],
                                featuresType: String
                               ) :DataFrame = {

    val baseDateCol: Column = baseDateSource match {
      case Left("now") => lit(nowDate)
      case Left(colName) => lit(colName)
      case Right(column) => column
    }

    val features = featuresType match {
      case "counts" => generateCountFeatures(baseTable, dateCol, grpByCol, pivotCol, lib, milestones, baseDateCol)
      case "countsplussums" => generateCountAndSumsFeatures(baseTable, dateCol, valCol, grpByCol, pivotCol, lib, milestones, baseDateCol)
      case "valuetrends" => generateValueTrendsFeatures(baseTable, dateCol, valCol, grpByCol, pivotCol, intervals, baseDateCol)
    }

    features
  }

  def generateCountFeatures(baseTable: DataFrame,
                            dateCol:String,
                            grpByCol:String,
                            pivotCol:String,
                            lib:String="",
                            milestones:Seq[Int],
                            baseDateCol:Column
                           ) :DataFrame = {


    baseTable
      .withColumn("basedate", baseDateCol)
      .select(dateCol, grpByCol, pivotCol, "basedate")
      .columnValuesToString(grpByCol)
      .getCountEventsWithinMilestonesFeatures(dateCol, "basedate", grpByCol, pivotCol, lib, milestones)
  }

  def generateCountAndSumsFeatures(baseTable: DataFrame,
                                   dateCol:String,
                                   valCol:String,
                                  grpByCol:String,
                                  pivotCol:String,
                                  lib:String="",
                                  milestones:Seq[Int],
                                   baseDateCol:Column
                                  ) :DataFrame = {

    baseTable
      .withColumn("basedate", baseDateCol)
      .select(dateCol, valCol, grpByCol, pivotCol, "basedate")
      .columnValuesToString(grpByCol)
      .getCountsSumsValuesWithinMilestonesFeatures(dateCol, "basedate", valCol, grpByCol, pivotCol, lib, milestones)
  }

  def generateValueTrendsFeatures(baseTable: DataFrame,
                                  dateCol:String,
                                  valCol:String,
                                  grpByCol:String,
                                  pivotCol:String,
                                  intervals:Seq[(Int, Int, Int, Int)],
                                  baseDateCol:Column
                                 ):DataFrame = {
    baseTable
      .withColumn("basedate", baseDateCol)
      .select(dateCol, valCol, grpByCol, pivotCol, "basedate")
      .columnValuesToString(grpByCol)
      .getValueTrendFeatures(dateCol, "basedate", valCol, grpByCol, pivotCol, intervals)
  }

}
