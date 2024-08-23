package usecaseexample

import DomainTables.CardUsageRefTable
import generators.BaseDateFeaturesGenerator.{countsplussums, valuetrends}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import usecaseexample.Churners.getChurnLearningTarget

object UCRunExample {

  def main(args: Array[String]): Unit = {

    try {
      val churners: DataFrame = getChurnLearningTarget()
      val cardProcessor = new CardUsageProcessor()

      val processedData: DataFrame = processCardUsageDF(cardProcessor)

      displayResults(processedData, cardProcessor)
    } catch {
      case e: Exception =>
        println(s"Something's not working: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  private def processCardUsageDF(cardProcessor: CardUsageProcessor) : DataFrame = {

    lazy val cardDF = CardUsageRefTable.getCardUsageDF
      .persist(StorageLevel.MEMORY_AND_DISK)

    try {
      val pivotedTable: DataFrame = cardProcessor.createPivotTable(cardDF, "lieu")
      val churners: DataFrame = getChurnLearningTarget()

      cardProcessor.joinLearningTargetTable(pivotedTable, "id_client", churners, "client_id")
    } finally {
      cardDF.unpersist()
    }

  }

  private def displayResults (generationDF: DataFrame, cardProcessor: CardUsageProcessor): Unit = {

    println()
    println("DataFrame on which feature generation will be based:")
    generationDF.show()

    println("Learning features (counts plus sums):")
    cardProcessor.generateLearningFeatures(generationDF, countsplussums.featuretype).show()

    println("Learning features (value trends):")
    cardProcessor.generateLearningFeatures(generationDF, valuetrends.featuretype).show()

    println("Inference features (counts plus sums):")
    cardProcessor.generateInferenceFeatures(generationDF, countsplussums.featuretype).show()

  }

}
