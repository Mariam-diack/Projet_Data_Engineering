package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataValidation {

  /**
   * 1. VALIDATION TRANSACTIONS
   * Règles : amount > 0 et timestamp comportant 14 caractères.
   * Renvoie un couple (Dataset[Transaction] Valides, DataFrame Rejetés avec motif)
   */
  def validateTransactions(transactions: Dataset[Transaction]): (Dataset[Transaction], DataFrame) = {
    val rawDf = transactions.toDF()

    // Correction apportée : Utilisation simple de .isNull sur la colonne plate 🌟
    val invalidAmount = col("amount").isNull || col("amount") <= ConfigLoader.minAmount
    val invalidTimestamp = col("timestamp").isNull || length(col("timestamp")) =!= ConfigLoader.timestampLength

    // Extraction et enrichissement des rejets
    val rejectedDf = rawDf.filter(invalidAmount || invalidTimestamp)
      .withColumn("rejection_reason",
        when(invalidAmount && invalidTimestamp, "Montant <= 0 ET Longueur Timestamp != 14")
          .when(invalidAmount, "Montant inférieur ou égal à 0")
          .otherwise("Longueur du Timestamp différente de 14")
      )

    // Extraction des lignes valides (Dataset fortement typé)
    val validDs = transactions.filter(tx =>
      tx.amount.getOrElse(0.0) > ConfigLoader.minAmount &&
        tx.timestamp != null &&
        tx.timestamp.length == ConfigLoader.timestampLength
    )

    (validDs, rejectedDf)
  }

  /**
   * 2. VALIDATION UTILISATEURS
   * Règles : age compris entre 16 et 100, et annual_income > 0.
   * Renvoie un couple (Dataset[User] Valides, DataFrame Rejetés avec motif)
   */
  def validateUsers(users: Dataset[User]): (Dataset[User], DataFrame) = {
    val rawDf = users.toDF()

    // Correction apportée : Suppression de .getField("value") 🌟
    val invalidAge = col("age").isNull || col("age") < ConfigLoader.minAge || col("age") > ConfigLoader.maxAge
    val invalidIncome = col("annual_income").isNull || col("annual_income") <= ConfigLoader.minIncome

    val rejectedDf = rawDf.filter(invalidAge || invalidIncome)
      .withColumn("rejection_reason",
        when(invalidAge && invalidIncome, "Âge hors [16-100] ET Revenu <= 0")
          .when(invalidAge, "Âge hors de la plage autorisée [16-100]")
          .otherwise("Revenu annuel inférieur ou égal à 0")
      )

    val validDs = users.filter(u =>
      u.age.getOrElse(0L) >= ConfigLoader.minAge &&
        u.age.getOrElse(0L) <= ConfigLoader.maxAge &&
        u.annual_income.getOrElse(0.0) > ConfigLoader.minIncome
    )

    (validDs, rejectedDf)
  }

  /**
   * 3. VALIDATION PRODUITS
   * Règles : price > 0 et rating compris entre 1 et 5.
   * Renvoie un couple (Dataset[Product] Valides, DataFrame Rejetés avec motif)
   */
  def validateProducts(products: Dataset[Product]): (Dataset[Product], DataFrame) = {
    val rawDf = products.toDF()

    // Correction apportée : Suppression de .getField("value") 🌟
    val invalidPrice = col("price").isNull || col("price") <= ConfigLoader.minPrice
    val invalidRating = col("rating").isNull || col("rating") < ConfigLoader.minRating || col("rating") > ConfigLoader.maxRating

    val rejectedDf = rawDf.filter(invalidPrice || invalidRating)
      .withColumn("rejection_reason",
        when(invalidPrice && invalidRating, "Prix <= 0 ET Note hors [1-5]")
          .when(invalidPrice, "Prix inférieur ou égal à 0")
          .otherwise("Note (rating) en dehors de la plage [1-5]")
      )

    val validDs = products.filter(p =>
      p.price.getOrElse(0.0) > ConfigLoader.minPrice &&
        p.rating.getOrElse(0.0) >= ConfigLoader.minRating &&
        p.rating.getOrElse(0.0) <= ConfigLoader.maxRating
    )

    (validDs, rejectedDf)
  }

  /**
   * 4. VALIDATION COMMERÇANTS
   * Règles : commission_rate compris entre 0 et 1.
   * Renvoie un couple (Dataset[Merchant] Valides, DataFrame Rejetés avec motif)
   */
  def validateMerchants(merchants: Dataset[Merchant]): (Dataset[Merchant], DataFrame) = {
    val rawDf = merchants.toDF()

    // Correction apportée : Suppression de .getField("value") 🌟
    val invalidCommission = col("commission_rate").isNull || col("commission_rate") < ConfigLoader.minCommission || col("commission_rate") > ConfigLoader.maxCommission

    val rejectedDf = rawDf.filter(invalidCommission)
      .withColumn("rejection_reason", lit("Taux de commission en dehors de la plage [0-1]"))

    val validDs = merchants.filter(m =>
      m.commission_rate.getOrElse(0.0) >= ConfigLoader.minCommission &&
        m.commission_rate.getOrElse(0.0) <= ConfigLoader.maxCommission
    )

    (validDs, rejectedDf)
  }

  /**
   * 5. STRATÉGIE ET BILAN DE QUALITÉ GLOBALE
   */
  def getDatasetSummary(spark: SparkSession, name: String, rawDf: DataFrame, validCount: Long, rejectedDf: DataFrame): DataFrame = {
    val totalRead = rawDf.count()
    val totalRejected = rejectedDf.count()

    val rejectionRate = if (totalRead > 0) {
      BigDecimal((totalRejected.toDouble / totalRead.toDouble) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    } else 0.0

    val totalNulls = rawDf.rdd.map(row => {
      var nulls = 0L
      for (i <- 0 until row.length) {
        if (row.isNullAt(i) || row.get(i) == null) nulls += 1
      }
      nulls
    }).sum().toLong

    spark.createDataFrame(Seq(
      (name, totalRead, validCount, totalRejected, rejectionRate, totalNulls)
    )).toDF("dataset", "nb_lignes_lues", "nb_lignes_valides", "nb_lignes_rejetees", "taux_rejet", "nb_valeurs_nulles")
  }

} // Fin de la classe DataValidation
