import featuressuite.EventFeatureEngineeringImplicits.EventBasedFeatureEngineeringFunctions
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper
import org.scalactic.TolerantNumerics
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import toolkits.DataLoading

import scala.collection.Seq

class EventBasedFeatureEngineeringFunctionsTest extends AnyFunSuite {

  val eps = 1e-4f
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(eps)

  val test_df = DataLoading.loadCsv("generator/src/test/resources/data-engineering-dataset.csv")

  test("getPivotedCountsAndSumsFeatures should calculate correct sums") {
    val result = test_df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getPivotedCountsAndSumsFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", "mt", Seq(1, 3, 6))
      .na.fill(0)

    withClue("Result should have 2 rows:") {
      result.count() shouldBe 2
    }

    withClue("Sum for client sgjuL0CYJf should be correct:") {
      val sum = result.select("GAB_MAROC_retrait_gab_sum_mt_6m")
        .filter(col("id_client") === "sgjuL0CYJf")
        .first().getDouble(0)
      sum shouldBe 1700.0 +- eps
    }

    withClue("Sum for client 17AbPYC2M7 should be correct:") {
      val sum = result.select("GAB_MAROC_retrait_gab_sum_mt_6m")
        .filter(col("id_client") === "17AbPYC2M7")
        .first().getDouble(0)
      sum shouldBe 500.0 +- eps
    }
  }

  test("getPivotedCountsAndSumsFeatures should yield correct column names") {
    val result = test_df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getPivotedCountsAndSumsFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", "mt", Seq(1, 3, 6))
      .na.fill(0)

    val expectColumns = Seq("id_client",
      "GAB_MAROC_consultation_solde_sum_mt_1m",
      "GAB_MAROC_consultation_solde_sum_mt_3m",
      "GAB_MAROC_consultation_solde_sum_mt_6m",
      "GAB_MAROC_retrait_gab_sum_mt_1m",
      "GAB_MAROC_retrait_gab_sum_mt_3m",
      "GAB_MAROC_retrait_gab_sum_mt_6m",
      "INTERNATIONAL_paiement_tpe_sum_mt_1m",
      "INTERNATIONAL_paiement_tpe_sum_mt_3m",
      "INTERNATIONAL_paiement_tpe_sum_mt_6m",
      "MAROC_paiement_tpe_sum_mt_1m",
      "MAROC_paiement_tpe_sum_mt_3m",
      "MAROC_paiement_tpe_sum_mt_6m",
      "MAROC_pin_errone_sum_mt_1m",
      "MAROC_pin_errone_sum_mt_3m",
      "MAROC_pin_errone_sum_mt_6m")


    assert(test_df.count() == 7, "Input CSV seems correct")
    assert(result.count() == 2, "Output dataframe should contain 2 entries")
    expectColumns.foreach(c => assert(result.columns.contains(c), f"Pivot behavior for column ${c} is correct"))
  }

  test("getPivotedCountsFeatures should yield correct column names") {
    val result = test_df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getPivotedCountsFeatures("timestamp_interaction", "basedt", "id_client", "pivot", "", Seq(1, 3, 6))
      .na.fill(0)

    val expectColumns = Seq("id_client",
      "GAB_MAROC_consultation_solde_nb_1m",
      "GAB_MAROC_consultation_solde_nb_3m",
      "GAB_MAROC_consultation_solde_nb_6m",
      "GAB_MAROC_retrait_gab_nb_1m",
      "GAB_MAROC_retrait_gab_nb_3m",
      "GAB_MAROC_retrait_gab_nb_6m",
      "INTERNATIONAL_paiement_tpe_nb_1m",
      "INTERNATIONAL_paiement_tpe_nb_3m",
      "INTERNATIONAL_paiement_tpe_nb_6m",
      "MAROC_paiement_tpe_nb_1m",
      "MAROC_paiement_tpe_nb_3m",
      "MAROC_paiement_tpe_nb_6m",
      "MAROC_pin_errone_nb_1m",
      "MAROC_pin_errone_nb_3m",
      "MAROC_pin_errone_nb_6m")

    assert(test_df.count() == 7, "Input CSV seems correct")
    assert(result.count() == 2, "Output dataframe should contain 2 entries")
    expectColumns.foreach(c => assert(result.columns.contains(c), f"Pivot behavior for column ${c} is correct"))
  }

  test("getPivotedCountsFeatures should calculate correct sums") {
    val result = test_df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getPivotedCountsFeatures("timestamp_interaction", "basedt", "id_client", "pivot", "", Seq(1, 3, 6))
      .na.fill(0)

    withClue("Result should have 2 rows:") {
      result.count() shouldBe 2
    }

    withClue("Count for client sgjuL0CYJf should be correct:") {
      val count = result.select("GAB_MAROC_retrait_gab_nb_6m")
        .filter(col("id_client") === "sgjuL0CYJf")
        .first().getLong(0)
      count shouldBe 2
    }

    withClue("Count for client 17AbPYC2M7 should be correct:") {
      val count = result.select("GAB_MAROC_retrait_gab_nb_6m")
        .filter(col("id_client") === "17AbPYC2M7")
        .first().getLong(0)
      count shouldBe 1
    }
  }

  test("getRatioPivotFeatures should yield correct column names") {
    val result = test_df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getPivotedValueTrendFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", Seq((1, 6, 6, 12)))
      .na.fill(0)

    val expectColumns = Seq("id_client",
      "GAB_MAROC_consultation_solde_ratio_1_6_6_12",
      "GAB_MAROC_retrait_gab_ratio_1_6_6_12",
      "INTERNATIONAL_paiement_tpe_ratio_1_6_6_12",
      "MAROC_paiement_tpe_ratio_1_6_6_12",
      "MAROC_pin_errone_ratio_1_6_6_12")

    assert(test_df.count() == 7, "Input CSV seems correct")
    assert(result.count() == 2, "Output dataframe should contain 2 entries")
    expectColumns.foreach(c => assert(result.columns.contains(c), f"Pivot behavior for column ${c} is correct"))
  }



  test("getRatioPivotFeatures should calculate correct ratios") {
    val result = test_df
      .withColumn("basedt", lit("2019-07-01"))
      .withColumn("pivot", concat_ws("_", col("lieu_interaction"), col("type_interaction")))
      .getPivotedValueTrendFeatures("timestamp_interaction", "basedt", "montant_dhs", "id_client", "pivot", Seq((1, 6, 6, 12)))
      .na.fill(0)

    withClue("Result should have 2 rows:") {
      result.count() shouldBe 2
    }

    withClue("Ratio for client sgjuL0CYJf should be correct:") {
      val ratio = result.select("GAB_MAROC_retrait_gab_ratio_1_6_6_12")
        .filter(col("id_client") === "sgjuL0CYJf")
        .first().getDouble(0)
      ratio shouldBe 5.3033 +- eps
    }

    withClue("Ratio for client 17AbPYC2M7 should be correct:") {
      val ratio = result.select("GAB_MAROC_retrait_gab_ratio_1_6_6_12")
        .filter(col("id_client") === "17AbPYC2M7")
        .first().getDouble(0)
      ratio shouldBe 6.2166 +- eps
    }
  }


}
