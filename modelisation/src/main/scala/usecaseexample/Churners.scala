package usecaseexample

import org.apache.spark.sql.DataFrame
import toolkits.DataLoading

object Churners {

  def getChurnLearningTarget() : DataFrame =
    DataLoading.loadCsv("modelisation/src/main/scala/usecaseexample/churners.csv")
      .select(
        "client_id",
        "churn_segment",
        "churn_date"
      )

}
