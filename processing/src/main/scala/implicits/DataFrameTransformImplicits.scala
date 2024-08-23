package implicits

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, percent_rank, rand, rank, regexp_replace}
import org.apache.spark.sql.types.FloatType

object DataFrameTransformImplicits {

  implicit class DataFrameTransformFunctions (df:DataFrame) {


    /**
     * A simple if implicit if-else :p
     * @param condition if(condition)
     * @param dataFrameTrans then apply(dataFrameTrans)
     * @return if (condition) transform(df) else df
     */
    def conditionnalTransform(condition:Boolean)(dataFrameTrans: DataFrame => DataFrame) : DataFrame = {
      if (condition) df.transform(dataFrameTrans) else df
    }

    /**
     * Returns a dataframe with only the first partitionCol element given a sort order orderCols
     * @param partitionCols columns used to partition the window, equivalent to a groupBy
     * @param orderCols columns used to sort each partition (with asc or desc functions)
     * @note the rand() function is used in case orderCols in empty. There may be a better way
     * @example df.getTop(col("client_id"))(desc("purchaseDate"))
     *          will return one of the latest purchases for each client
     */
    def getTop(partitionCols:Column*)(orderCols: Column*) = {
      df.withColumn("rank", rank.over(
          Window
            .partitionBy(partitionCols:_*)
            .orderBy(orderCols :+ rand() :_*)))
        .where(col("rank") === 1)
        .drop("rank")
    }

    /**
     * Returns a dataframe with only the first partitionCol element given a sort order orderCols
     * @param partitionCols columns used to partition the window, equivalent to a groupBy
     * @param orderCols columns used to sort each partition (with asc or desc functions)
     * @param howMany the number of top elements we want
     * @note the rand() function is used in case orderCols in empty. There may be a better way
     * @example df.getTops(col("client_id"))(5, desc("purchaseDate"), asc("itemPrice"))
     *          will return the 5 last/costly purchases for each client
     */
    def getTops(partitionCols:Column*)(howMany: Int, orderCols: Column*) = {
      df
        .withColumn("rank", rank.over(
          Window
            .partitionBy(partitionCols:_*)
            .orderBy(orderCols :+ rand() :_*)))
        .where(col("rank") <= howMany)
    }

    /**
     * Returns a dataframe with only the first partitionCol element given a sort order orderCols
     * @param partitionCols columns used to partition the window, equivalent to a groupBy
     * @param orderCols columns used to sort each partition (with asc or desc functions)
     * @param percent the percentage of top elements we want
     * @note the rand() function is used in case orderCols in empty. There may be a better way
     * @example df.getTopsPercent(col("client_id"))(0.05, desc("purchaseDate"), asc("itemPrice"))
     *          will return the 5% last/costly purchases for each client
     */
    def getTopsPercent(partitionCols:Column*)(percent: Double, orderCols: Column*) = {
      df
        .withColumn("rank", percent_rank.over(
          Window
            .partitionBy(partitionCols:_*)
            .orderBy(orderCols :+ rand() :_*)))
        .where(col("rank") <= percent)
    }


    /**
     * Drop columns that contains a specific pattern in a DataFrame
     * @param pattern used to drop columns
     * @return the same dataframe without the matched columns
     */
    def dropColsContaining(pattern:String): DataFrame = {
      df.columns.filter(_.contains(pattern)).foldLeft(df) {
        case (df_arg, col) => df_arg.drop(col)
      }
    }


    /**
     * Replace ',' with '.' and then cast to float to convert string columns to float ones
     * @param cols columns to cast
     * @return the same dataframe with float columns
     */
    def applyCastToFloat(cols: String*): DataFrame = {
      cols.foldLeft(df) {
        case (df_arg, c) => df_arg
          .withColumn(c, regexp_replace(col(c), ",", ".").cast(FloatType))
      }
    }

    /**
     * Computes a lazy union, which is a union of two dataframe based
     * on intersection of columns without throwing error if schemas do not match.
     * Used when the input dataframes columns are not controlled (for example when parsing a csv file)
     * @param df_arg the other dataframe to union
     * @return the lazy union of both dataframes
     */
    def lazyUnion(df_arg:DataFrame): DataFrame = {
      val cols_intersect = df.columns.intersect(df_arg.columns)
      df.select(cols_intersect.map(col): _*).union(df_arg.select(cols_intersect.map(col): _*))
    }

    /**
     * Simple renaming shorter method for readability
     * @see withColumnRenamed(existingName: String, newName: String)
     */
    def wcr(existingName:String, newName:String) = df.withColumnRenamed(existingName, newName)


  }

}
