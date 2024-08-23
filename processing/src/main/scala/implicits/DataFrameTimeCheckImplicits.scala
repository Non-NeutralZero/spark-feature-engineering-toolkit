package implicits

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame


object DataFrameTimeCheckImplicits {

  implicit class DataFrameTimeCheckFunctions(df: DataFrame) {

    /**
     * Returns a new column 0/1 exist_a_b for each row which satisfies
     * eventDate € [baseDate - b*Month, baseDate - a*Month]
     * given an (a, b) interval
     * @param eventDateCol column used as the eventDate
     * @param baseDate column used as a date reference for the row
     *                 (each row may have its own reference when doing feature engineering)
     * @param intervals (a,b) used to build the [baseDate - b*Month, baseDate - a*Month] interval
     * @note warning (a,b) should satisfie : a < b
     */
    def checkDateWithinMonthInterval(eventDateCol:String, baseDate:String, intervals:(Int, Int)*): DataFrame = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn("exist_" + c._1 + "_" + c._2,
          (add_months(col(eventDateCol), c._2) > col(baseDate)
            && add_months(col(eventDateCol), c._1) < col(baseDate)).cast(IntegerType)) }
    }

    /**
     * Returns a new column 0/1 exist_b for each row which satisfies
     * eventDate € [baseDate - b*Month, baseDate] given a number of months b
     * @param eventDateCol column used as the eventDate
     * @param baseDate column used as a date reference for the row
     *                 (each row may have its own reference when doing feature engineering)
     * @param monthsNb b used to build the [baseDate - b*Month, baseDate] interval
     */
    def checkDateWithinMonthMilestone(eventDateCol:String, baseDate:String, monthsNb:Int*): DataFrame = {
      monthsNb.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn("exist_" + c,
          (add_months(col(eventDateCol), c) >= col(baseDate)
            && col(eventDateCol) < col(baseDate)).cast(IntegerType)) }
    }

    /**
     * Returns a new column lib_a_b that contains the value of valColName for each row which satisfies
     * eventDate € [baseDate - b*Month, baseDate - a*Month]
     * given an (a, b) interval
     * @param eventDateCol column used as the eventDate
     * @param baseDate column used as a date reference for the row
     *                 (each row may have its own reference when doing feature engineering)
     * @param valColName the value used to build the new column
     * @param lib a prefix for the column name
     * @param intervals (a,b) used to build the [baseDate - b*Month, baseDate - a*Month] interval
     * @note warning (a,b) should satisfie : a < b
     */
    def checkValWithinMonthInterval(eventDateCol:String, baseDate:String, valColName:String, lib: String,
                                    intervals:(Int, Int)*) = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn(lib + "_" + c._1 + "_" + c._2, col(valColName) *
          (add_months(col(eventDateCol), c._2) > col(baseDate)
            && add_months(col(eventDateCol), c._1) < col(baseDate)).cast(IntegerType))
      }
    }

    /**
     * Returns a new column lib_a_b that contains the value of valColName for each row which satisfies
     * eventDate € [baseDate - b*Month, baseDate]
     * given an (a, b) interval
     * @param eventDateCol column used as the eventDate
     * @param baseDate column used as a date reference for the row
     *                 (each row may have its own reference when doing feature engineering)
     * @param valColName the value used to build the new column
     * @param lib a prefix for the column name
     * @param monthsNb b used to build the [baseDate - b*Month, baseDate] interval
     * @note warning (a,b) should satisfie : a < b
     */
    def checkValWithinDateMonthMilestone(eventDateCol:String, baseDate:String, valColName:String, lib: String,
                                         monthsNb:Int*) = {
      monthsNb.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn(lib + "_" + c, col(valColName) *
          (add_months(col(eventDateCol), c) >= col(baseDate)
            && col(eventDateCol) < col(baseDate)).cast(IntegerType)) }
    }

    /**
     * A way to use two intervals in the same time for building ratio features
     * @see valDateMonthInterval(eventDateCol:String, baseDate:String, valColName:String, lib: String,
     *      intervals:(Int, Int)*)
     */
    def checkValWithinDateMonthDoubleInterval(eventDate:String, baseDate:String, colVal:String,
                                              lib:String, doubleIntervals:(Int, Int, Int, Int)*) = {
      df
        .checkValWithinMonthInterval(eventDate, baseDate, colVal, lib, doubleIntervals.map(i => (i._1, i._2)):_*)
        .checkValWithinMonthInterval(eventDate, baseDate, colVal, lib, doubleIntervals.map(i => (i._3, i._4)):_*)
    }


  }
}
