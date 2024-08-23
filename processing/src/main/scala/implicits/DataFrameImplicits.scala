package implicits

import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import java.sql.Date
import java.util.Calendar


object DataFrameImplicits {


  implicit class DataFrameImprovements(df: DataFrame) {

    /**
     * Filter a month / year based dataframe  given a baseCalendar (often now)
     * @note  Calendar.MONTH is 0 based (we add the 1+ in the filtering process)
     * @param baseCalendar java.util.Calendar to use for month/year filtering
     * @param colMonth column corresponding to the month
     * @param colYear column correspond to the year
     * @return A filtered dataframe
     */
    def projectMonthYearNow(baseCalendar:Calendar, colMonth:String, colYear:String) =
      df.filter(col(colYear) === baseCalendar.get(Calendar.YEAR) &&
        col(colMonth) === 1+baseCalendar.get(Calendar.MONTH))

    /**
     * Filter a month / year based dataframe  given a start and end date
     * @note Calendar.MONTH is 0 based (we add the 1+ in the filtering process)
     * @note We assume the dataframe day is 01
     * @param dateStart java.sql.Date start date
     * @param dateEnd java.sql.Date end date
     * @param colMonth column corresponding to the month
     * @param colYear column correspond to the year
     * @return A filtered dataframe
     */
    def borneMonthYear(dateStart:Date, dateEnd:Date, colMonth:String, colYear:String) =
      df.withColumn("date_tmp", to_date(concat_ws("-", col(colYear), col(colMonth), lit("01"))))
        .filter(col("date_tmp") >= lit(dateStart) && col("date_tmp") <= lit(dateEnd))
        .drop("date_tmp")

    /**
     * Given a spark window, we just lag a variable multiple times
     * @param w the spark window to use
     * @param colName the lagged column
     * @param vals the integer lag values
     * @return a dataframe with new lagged columns
     */
    def addLag(w:WindowSpec, colName:String, vals:List[Int]) = {
      vals.foldLeft(df) {
        case (df_arg, v) => df_arg.withColumn(colName + "_lag_" + v.toString, lag(colName, v).over(w))
      }
    }

    /**
     * Replacing values in a colum from a Map and leaving unchanged non existant keys in the map
     * Similar to Oracle decode function but with no default value
     * @param colToDecode column used to apply the decode
     * @param mapDecode map with key and values to replace by
     * @return a dataframe with new values in colToDecode column
     * @note there may be a more effecient way to do it whith an udf rather than "for" loop
     */
    def mapValues(colToDecode:String, mapDecode:Map[String, String]) = {
      mapDecode.foldLeft(df) {
        case (df_arg, kv) =>
          df_arg.withColumn(colToDecode, when(col(colToDecode) === kv._1, kv._2).otherwise(col(colToDecode)))
      }
    }

    /**
     * Simple getOrElse spark dataframe
     * @param newColName name of the new created column
     * @param getColName name of the column we want to get non null values
     * @param elseColName name of the columns from which values are going to be used if getColName values are null
     * @return a dataframe with a new getOrNull column
     */
    def withColumnGetOrElse(newColName:String, getColName:String, elseColName:String) =
      df.withColumn(newColName, when(col(getColName).isNotNull, col(getColName))
        .otherwise(col(elseColName)))

    /**
     * Simply overwriting column with a boolean condition
     * @param overrideColName name of the new created column
     * @param conditionBoolCol boolean column used as a condition on which you replace
     * @param strIfCond value litteral to replace with
     * @return If conditionBoolCol, then replace with strIfCond, else still use overrideColName
     */
    def overwriteColumnCondition(overrideColName:String, conditionBoolCol:Column, strIfCond:String) =
      df.withColumn(overrideColName, when(conditionBoolCol, strIfCond).otherwise(col(overrideColName)))

    /**
     * Override a column using multiple conditions
     * @param overrideColName column name to override
     * @param mapConditionLit conditions in a map with a conditionBoolCol as a key and strIfConf as a value
     * @see overwriteColumnCondition(overrideColName:String, conditionBoolCol:Column, strIfCond:String)
     */
    def overrideColumnConditions(overrideColName:String, mapConditionLit:Map[Column, String]) =
      mapConditionLit.foldLeft(df) {
        case (df_arg, kv) => df_arg.overwriteColumnCondition(overrideColName, kv._1, kv._2)
      }

    /**
     * Implements an Oracle Decode in spark using a Map of "switch case conditions". Null values are not handled
     * @param newColName new column name
     * @param decodeColName column on which decode will be applied
     * @param decodeMap contains
     *                  _1 : value to be tested against decodeColName
     *                  _2 : litteral '''string''' to be used if the previous test has matched
     * @param fallBackCol column value used when no match has been found
     * @return a new column with a decoded column
     */
    def withColumnDecodeStringLit(newColName:String, decodeColName:String, decodeMap:Map[String, String],
                                  fallBackCol:Column=lit("")) =
      decodeMap.foldLeft(
        df.withColumn(newColName, fallBackCol)) {
        case (df_arg, kv) =>
          df_arg.withColumn(newColName, when(col(decodeColName) === lit(kv._1), lit(kv._2))
            .otherwise(col(newColName)))
      }

    /**
     * Implements an Oracle Decode in spark using a Map of "switch case conditions". Null values are not handled
     * @param newColName new column name
     * @param decodeColName column on which decode will be applied
     * @param decodeMap contains
     *                  _1 : value to be tested against decodeColName
     *                  _2 : litteral '''int''' to be used if the previous test has matched
     * @param fallBackCol column value used when no match has been found
     * @return a new column with a decoded column
     */
    def withColumnDecodeIntLit(newColName:String, decodeColName:String, decodeMap:Map[String, Int],
                               fallBackCol:Column=lit(0)) =
      decodeMap.foldLeft(
        df.withColumn(newColName, fallBackCol)
      ) {
        case (df_arg, kv) => df_arg.withColumn(newColName, when(col(decodeColName) === lit(kv._1), lit(kv._2))
          .otherwise(col(newColName)))
      }

    /**
     * Implements an Oracle Decode in spark using a Map of "switch case conditions". Null values are not handled
     * @param newColName new column name
     * @param decodeColName column on which decode will be applied
     * @param decodeMap contains
     *                  _1 : value to be tested against decodeColName
     *                  _2 : column name indicating the colum to be used if the previous test has matched
     * @param fallBackCol column value used when no match has been found
     * @return a new column with a decoded column
     */
    def withColumnDecodeCol(newColName:String, decodeColName:String, decodeMap:Map[String, String],
                            fallBackCol:Column=lit("")) =
      decodeMap.foldLeft(
        df.withColumn(newColName, fallBackCol)
      ) {
        case (df_arg, kv) => df_arg.withColumn(newColName, when(col(decodeColName) === lit(kv._1), col(kv._2))
          .otherwise(col(newColName)))
      }

  }

}