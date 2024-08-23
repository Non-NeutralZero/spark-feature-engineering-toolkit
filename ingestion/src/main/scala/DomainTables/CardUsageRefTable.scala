package DomainTables

import org.apache.spark.sql.DataFrame
import toolkits.DataLoading

object CardUsageRefTable {

  def getCardUsageDF : DataFrame = {

    DataLoading.loadCsv("ingestion/tables/cardusage.csv")
      .select(
        "id_client",
        "timestamp_interaction",
        "montant_dhs",
        "lieu",
        "type"
      )
  }


  }
