package implicits

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lower, regexp_replace}
import org.apache.spark.sql.types.DataTypes

object DataFrameStandardizingImplicits {


  implicit class DataFrameStandardizingFunctions (df:DataFrame) {

    /**
     * Helps renaming agrgegated columns to something more readable
     * "exist" is an indicator that tells if a value has been found.
     * It is build with a .withColumn(booleanColumn.cast(IntegerType))
     * "v" is the numeric value
     * Developed to be mainly used by [[renameColumnsWithAggHeuristic]]
     * @param col : column to rename
     * @param lib : renamed column context (transfer, deposit, call etc.)
     * @example Column "sum(exist_6)" will be converted to "nb_6_month"
     *          Column "avg(v_24)" will be converted to "avg_24_month"
     * @return
     */
    private def replaceAggFunctionsHeuristic(col: String, lib: String) = {
      val lib2 = if (lib.isEmpty) { "" } else {"_"+lib}
      col
        .replaceAll("sum\\(exist_(\\d+)\\)", "nb" + lib2 + "_$1m")
        .replaceAll("sum\\(v_(\\d+)\\)", "sum" + lib2 + "_$1m")
        .replaceAll("avg\\(v_(\\d+)\\)", "avg" + lib2 + "_$1m")
        .replaceAll("max\\(v_(\\d+)\\)", "max" + lib2 + "_$1m")
    }

    /**
     * Uses the replaceAggFunctionsHeuristic to change the name of columns
     * @param lib renamed column context (transfer, deposit, call etc.)
     * @return the same dataframe with the replaced columns
     */
    def renameColumnsWithAggHeuristic(lib:String=""): DataFrame = {
      df.columns.foldLeft(df) {
        case (df_arg, col) => df_arg.withColumnRenamed(col, replaceAggFunctionsHeuristic(col, lib))
      }
    }


    /**
     * Converts column values to string
     * Works mostly as a guard
     * @param colName
     * @return column in String type
     */
    def columnValuesToString(colName: String): DataFrame = {
      df.withColumn(colName, col(colName).cast(DataTypes.StringType))
    }

    /** Removes non-word characters (such as punctuation marks, spaces, etc.) from
     * the values in each of the specified columns in [[colNames]].
     *
     * @param colNames: sequence of column names
     * @return data cleaning by creating new columns with the cleaned values.
     * @example Original DataFrame df:
     *      {{{
     *  +----------------------+----------------+
     * |                col1|               col2|
     * +-----------------------+----------------+
     * |    local tpe;;= ser...|     bla bla bla|
     * |   local data-$ service|   bla bla   bla|
     * |  local data 33 service|   bla bla ::bla|
     * |     local data service|     bla bla bla|
     * |     local data service|bla      bla bla|
     * +-----------------------+----------------+
     *       }}}
     *
     *      After applying df.cleanColumnValues(Seq("col1", "col2")) the transformed df will become:
     *       {{{
                 +------------------+---------+
            |              col1|     col2|
            +------------------+---------+
            |   localtpeservice|blablabla|
            |  localdataservice|blablabla|
            |local33dataservice|blablabla|
            |  localdataservice|blablabla|
            |  localdataservice|blablabla|
            +------------------+---------+
     *      }}}
     *
     */
    def cleanColumnValues(colNames: Seq[String]): DataFrame = {
      val regex = """\W+""".r.toString() // Regex pattern for non-word characters

      colNames.foldLeft(df) {
        case (df_arg, colName) =>
          df_arg
            .withColumn(colName, lower(regexp_replace(col(colName), regex, "")))
      }
    }


    /**
     * rename 'right' multiple column names
     * @param extension the right extension to add
     * @param cols the columns to change
     * @return A renamed DataFrame
     * @example df.renameExt('_right', 'col1', 'col2')
     *          will produce
     *          df.withColumnRenamed('col1', 'col1_right')
     *            .withColumnRenamed('col2', 'col2_right')
     */
    def renameRight(extension: String, cols: String*): DataFrame = {
      cols.foldLeft(df) { case (df_arg, col) => df_arg.withColumnRenamed(col, col + extension) }
    }

    /**
     * The same function as renameRight but applied on all the DataFrame columns
     * @see renameRight(extension: String, cols: String*)
     */
    def renameAllRight(extension: String): DataFrame = {
      df.columns.foldLeft(df) { case (df_arg, col) => df_arg.withColumnRenamed(col, col + extension) }
    }

    /**
     * Rename multiple columns using Map function
     * @param renExpr the couple "a" -> "b" that defines the input and output column
     * @return A renamed DataFrame
     * @example df.renameMap("ok" -> "ko", "ik" -> "ki")
     *          will produce
     *          df.withColumnRenamed('ok', 'ko')
     *            .withColumnRenamed('ik', 'ki')
     */
    def renameMap(renExpr: (String, String), renExprs: (String, String)*): DataFrame = {
      (renExpr +: renExprs).foldLeft(df) { case (df_arg, elem) => df_arg.withColumnRenamed(elem._1, elem._2) }
    }

    /**
     * Same as renameMap((String,String)*) but using a map instead
     * @see renameMap(renExpr: (String, String), renExprs: (String, String)*)
     */
    def renameMap(map: Map[String, String]): DataFrame = {
      map.foldLeft(df) { case (df_arg, elem) => df_arg.withColumnRenamed(elem._1, elem._2) }
    }



    /**
     * Same as renameMap of withColumnSubstringRenamed function
     * @see withColumnSubStrRenamed(existingSubStrColName:String, replaceBySubStrColName:String)
     */
    def renameSubStrMap(renExpr: (String, String), renExprs: (String, String)*): DataFrame = {
      (renExpr +: renExprs).foldLeft(df) { case (df_arg, elem) =>
        df_arg.withColumnSubStrRenamed(elem._1, elem._2) }
    }


    /**
     * Replace columns that contain a specific substring
     * @param existingSubStrColName Substring to look for in input columns
     * @param replaceBySubStrColName Replace pattern in output columns
     * @return
     *         Example : if column "abc12ab" exists
     *         Calling the code with parameters "12a" , "NEW"
     *         will rename the column "abc12ab" to "abcNEWb"
     */
    def withColumnSubStrRenamed(existingSubStrColName:String, replaceBySubStrColName:String): DataFrame = {
      df.columns.filter(_.contains(existingSubStrColName)).foldLeft(df) {
        case (df_arg, col) =>
          df_arg.withColumnRenamed(col, col.replace(existingSubStrColName, replaceBySubStrColName))
      }
    }


    /**
     * Drop multiple columns (more concise syntax)
     * @param cols columns to drop
     * @return the same dataframe with less columns
     */
    def drop(cols:Column*): DataFrame = {
      cols.foldLeft(df) {
        case (df_arg, col) => df_arg.drop(col)
      }
    }

    /**
     * Rename multiple columns with a suffix (left string extension)
     * @param extension for example '_left'
     * @param cols columns that will have the extension added
     * @return The same dataframe with renamed columns
     * @example df.renameExt('left_', 'col1', 'col2')
     *          will produce
     *          df.withColumnRenamed('col1', 'left_col1')
     *            .withColumnRenamed('col2', 'left_col2')
     */
    def renameLeft(extension: String, cols: String*): DataFrame = {
      cols.foldLeft(df) { case (df_arg, col) => df_arg.withColumnRenamed(col, extension + col) }
    }

    /**
     * Remove a pattern from column names. Used mainly to remove the annoying back tick when using special characters
     * @param pattern to use for the replace function
     * @param repl used to replace
     * @return the same dataframe with new column name
     */
    def repString(pattern: String, repl:String): DataFrame = {
      df.columns.foldLeft(df) { case (df_arg, c) => df_arg.withColumnRenamed(c, c.replace(pattern, repl)) }
    }

    /**
     * Remove a string for column names
     * @see repString(pattern: String, repl:String)
     */
    def remString(str: String): DataFrame = {
      df.repString(str, "")
    }


  }

}
