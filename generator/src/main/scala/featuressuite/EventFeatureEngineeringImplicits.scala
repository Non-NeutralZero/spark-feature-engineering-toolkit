package featuressuite

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import implicits.DataFrameImplicits.DataFrameImprovements
import implicits.DataFrameTimeCheckImplicits.DataFrameTimeCheckFunctions
import implicits.DataFrameTransformImplicits.DataFrameTransformFunctions
import implicits.DataFrameStandardizingImplicits.DataFrameStandardizingFunctions


/**
 * Those are the functions used for feature engineering
 */
object EventFeatureEngineeringImplicits {

  implicit class EventBasedFeatureEngineeringFunctions(df: DataFrame) {

    /**
     * Feature engineering function based on event happenning (dateCol), given a base date (baseDtCol)
     * With a group by that includes a pivot category
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column also used to group by
     * @param lib a small text included in newly created column names
     * @param milestones used to determine the event date check intervals
     * @return Original dataframe with new columns indicating number of events within milestones for each grpByCol and pivotCol
     * @example nb_1_month, nb_3_month
     */
    def getCountEventsWithinMilestonesFeatures(dateCol:String, baseDtCol:String,
                                               grpByCol:String, pivotCol:String,
                                               lib:String, milestones:Seq[Int]) = {
      df
        .select(grpByCol, pivotCol, dateCol, baseDtCol)
        .checkDateWithinMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .renameColumnsWithAggHeuristic(lib)
    }

    /**
     * Feature engineering function based on event happenning (dateCol) and an associated value (valCol),
     * given a base date (baseDtCol) with a group by then sum
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param valCol event value colum
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column also used to group by
     * @param lib a small text included in newly created column names
     * @param milestones used to determine the event date check intervals
     * @return Original dataframe with new columns indicating number of events and total of value associated with event,
     *         within milestones for each grpByCol and pivotCol
     */
    def getCountsSumsValuesWithinMilestonesFeatures(dateCol:String,
                                                    baseDtCol:String,
                                                    valCol:String,
                                                    grpByCol:String,
                                                    pivotCol:String,
                                                    lib:String,
                                                    milestones:Seq[Int]) = {
      df
        .select(grpByCol, pivotCol, valCol, dateCol, baseDtCol)
        .checkDateWithinMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .checkValWithinDateMonthMilestone(dateCol, baseDtCol, valCol, "v", milestones:_*)
        .drop(valCol)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .renameColumnsWithAggHeuristic(lib)
    }

    /**
     * Feature engineering function based on event happenning (dateCol), given a base date (baseDtCol)
     * With a group by then sum given a pivot column
     * @note the pivot() is adding new column names with the 'pivotCol' component when there is only one column
     *       hence the khouza3bila fake column to force it add the 'pivotCol'
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column used to group by sum pivot
     * @param lib a small text included in newly created column names
     * @param milestones used to determine the event date check intervals
     * @return exist pivoted features for an event DataFrame
     */
    def getPivotedCountsFeatures(dateCol:String, baseDtCol:String,
                                 grpByCol:String, pivotCol:String,
                                 lib:String, milestones:Seq[Int]) = {
      df
        .select(grpByCol, pivotCol, dateCol, baseDtCol)
        // khouza3bila (خزعبلة) means a "random/funny thing" in moroccan arabic
        .conditionnalTransform(milestones.length == 1)(_.withColumn("khouza3bila", lit(1)))
        .checkDateWithinMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .groupBy(grpByCol)
        .pivot(pivotCol)
        .sum()
        .conditionnalTransform(milestones.length == 1)(_.dropColsContaining("khouza3bila"))
        .renameColumnsWithAggHeuristic(lib)
    }

    /**
     * Feature engineering function based on event happenning (dateCol) and an associated value (valCol),
     * given a base date (baseDtCol) with a group by then sum given a pivot column
     * @note the pivot() is adding new column names with the 'pivotCol' component when there is only one column
     *       hence the khouza3bila fake column to force it add the 'pivotCol'
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param valCol event value colum
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column used to group by sum pivot
     * @param lib a small text included in newly created column names
     * @param milestones used to determine the event date check intervals
     * @return exist/value pivoted features for an event DataFrame
     */
    def getPivotedCountsAndSumsFeatures(dateCol:String, baseDtCol:String, valCol:String,
                                        grpByCol:String, pivotCol:String, lib:String, milestones:Seq[Int]) = {
      df
        .select(grpByCol, valCol, pivotCol, dateCol, baseDtCol)
        .conditionnalTransform(milestones.length == 1)(_.withColumn("khouza3bila", lit(1)))
        .checkDateWithinMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .checkValWithinDateMonthMilestone(dateCol, baseDtCol, valCol, "v", milestones:_*)
        .drop(valCol)
        .groupBy(grpByCol)
        .pivot(pivotCol)
        .sum()
        .conditionnalTransform(milestones.length == 1)(_.dropColsContaining("khouza3bila"))
        .renameColumnsWithAggHeuristic(lib)
    }

    /**
     * Calculate a log ratio between two intervals
     * @param lib usually tmp
     * @param intervals intervals used for calculating the base variables
     * @return a dataframe with a new ratio column
     */
    def getRatioCalulation(lib:String, intervals:(Int, Int, Int, Int)*) = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg
          .withColumn("ratio_"+c._1+"_"+c._2+"_"+c._3+"_"+c._4,
            log((col("sum("+ lib + "_"+c._1+"_"+c._2+")")+1)/ (col("sum("+ lib + "_"+c._3+"_"+c._4+")")+1)))
      }
    }

    /**
     * Build a ratio features dataframe base on multiple double intervals and an event based source dataframe
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param valCol event value colum
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column also used to group by
     * @param intervals the double intervals used to calculate the ratio
     * @return a dataframe with the new ratios
     */
    def getValueTrendFeatures(dateCol:String, baseDtCol:String, valCol:String,
                              grpByCol:String, pivotCol:String,
                              intervals:Seq[(Int, Int, Int, Int)]) = {
      df
        .select(grpByCol, pivotCol, valCol, dateCol, baseDtCol)
        .checkValWithinDateMonthDoubleInterval(dateCol, baseDtCol, valCol, "tmp", intervals:_*)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .getRatioCalulation("tmp", intervals:_*)
        .dropColsContaining("tmp")
        .drop("sum("+valCol+")")
    }

    /**
     * Build a ratio features dataframe base on multiple double intervals and an event based source dataframe
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param valCol event value colum
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column used to group by sum pivot
     * @param intervals the double intervals used to calculate the ratio
     * @return a pivoted dataframe with the new ratios
     */
    def getPivotedValueTrendFeatures(dateCol:String, baseDtCol:String, valCol:String,
                              grpByCol:String, pivotCol:String,
                              intervals:Seq[(Int, Int, Int, Int)]) = {
      df
        .conditionnalTransform(intervals.length == 1)(_.withColumn("khouza3bila", lit(1)))
        .checkValWithinDateMonthDoubleInterval(dateCol, baseDtCol, valCol, "tmp", intervals:_*)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .getRatioCalulation("tmp", intervals:_*)
        .groupBy(grpByCol)
        .pivot(pivotCol)
        .sum()
        .conditionnalTransform(intervals.length == 1)(_.dropColsContaining("khouza3bila"))
        .dropColsContaining("tmp").dropColsContaining("sum(sum")
        .remString("sum").remString("(").remString(")")
    }

  }

}
