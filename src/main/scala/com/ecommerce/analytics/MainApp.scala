package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.DataFrame

object MainApp {
  def main(args: Array[String]): Unit = {

    // Fix indispensable pour l'exécution Hadoop et winutils sous Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .master(ConfigLoader.sparkMaster)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions)
      .getOrCreate()

    // Nettoyage de la console : n'affiche que nos messages et avertissements métiers
    spark.sparkContext.setLogLevel("WARN")

    val ingestion = new DataIngestion(spark)
    val validation = new DataValidation()

    println("\n" + "="*75)
    println("=== SPARK : PIPELINE DE QUALITÉ COMPLET ET EXPORTATION DES DONNÉES ===")
    println("="*75 + "\n")

    // Liste pour collecter les résumés des jeux de données traités avec succès
    var summaryRows = Seq.empty[DataFrame]
    val outputBaseFolder = ConfigLoader.outputPath

    // =========================================================================
    // 1. PIPELINE DES TRANSACTIONS (CSV avec Schéma Strict)
    // =========================================================================
    try {
      val rawTx = ingestion.readTransactions()
      val countRead = rawTx.count()

      val (validTx, rejectedTx) = validation.validateTransactions(rawTx)
      val countValid = validTx.count()

      println(s" TRANSACTIONS | Lignes lues : $countRead | Lignes valides après filtrage : $countValid")
      validTx.show(5)

      // Matérialisation de l'échantillon physique en format Parquet (Sécurisé dans le try)
      val parquetTxPath = s"${outputBaseFolder}validated_transactions_parquet"
      println(s" Sauvegarde de l'échantillon Parquet (Transactions) dans : $parquetTxPath")
      validTx.write
        .mode("overwrite")
        .parquet(parquetTxPath)

      val txSummary = validation.getDatasetSummary(spark, "transactions", rawTx.toDF(), countValid, rejectedTx)
      summaryRows = summaryRows :+ txSummary
    } catch {
      case ex: org.apache.spark.sql.AnalysisException =>
        println(s" Erreur de structure / Schéma sur [Transactions] : ${ex.getMessage}")
      case ex: java.io.FileNotFoundException =>
        println(s" Fichier [Transactions] introuvable au chemin indiqué : ${ex.getMessage}")
      case ex: Exception =>
        println(s" Erreur lors du traitement des [Transactions] : ${ex.getMessage}")
    }

    // =========================================================================
    // 2. PIPELINE DES UTILISATEURS (JSON)
    // =========================================================================
    try {
      val rawUsers = ingestion.readUsers()
      val countRead = rawUsers.count()

      val (validUsers, rejectedUsers) = validation.validateUsers(rawUsers)
      val countValid = validUsers.count()

      println(s"\n UTILISATEURS | Lignes lues : $countRead | Lignes valides après filtrage : $countValid")
      validUsers.show(5)

      // Matérialisation de l'échantillon physique en format Parquet (Sécurisé dans le try)
      val parquetUsersPath = s"${outputBaseFolder}validated_users_parquet"
      println(s"💾 Sauvegarde de l'échantillon Parquet (Utilisateurs) dans : $parquetUsersPath")
      validUsers.write
        .mode("overwrite")
        .parquet(parquetUsersPath)

      val usersSummary = validation.getDatasetSummary(spark, "users", rawUsers.toDF(), countValid, rejectedUsers)
      summaryRows = summaryRows :+ usersSummary
    } catch {
      case ex: org.apache.spark.sql.AnalysisException =>
        println(s" Erreur de structure / Schéma sur [Utilisateurs] : ${ex.getMessage}")
      case ex: java.io.FileNotFoundException =>
        println(s" Fichier [Utilisateurs] introuvable au chemin indiqué : ${ex.getMessage}")
      case ex: Exception =>
        println(s" Erreur lors du traitement des [Utilisateurs] : ${ex.getMessage}")
    }

    // =========================================================================
    // 3. PIPELINE DES PRODUITS (PARQUET)
    // =========================================================================
    try {
      val rawProducts = ingestion.readProducts()
      val countRead = rawProducts.count()

      val (validProducts, rejectedProducts) = validation.validateProducts(rawProducts)
      val countValid = validProducts.count()

      println(s"\n PRODUITS     | Lignes lues : $countRead | Lignes valides après filtrage : $countValid")
      validProducts.show(5)

      val productsSummary = validation.getDatasetSummary(spark, "products", rawProducts.toDF(), countValid, rejectedProducts)
      summaryRows = summaryRows :+ productsSummary
    } catch {
      case ex: Exception => println(s" Erreur lors du traitement des [Produits] : ${ex.getMessage}")
    }

    // =========================================================================
    // 4. PIPELINE DES COMMERÇANTS (CSV avec Inférence de Schéma)
    // =========================================================================
    try {
      val rawMerchants = ingestion.readMerchants()
      val countRead = rawMerchants.count()

      val (validMerchants, rejectedMerchants) = validation.validateMerchants(rawMerchants)
      val countValid = validMerchants.count()

      println(s" COMMERÇANTS  | Lignes lues : $countRead | Lignes valides après filtrage : $countValid")
      validMerchants.show(5)

      val merchantsSummary = validation.getDatasetSummary(spark, "merchants", rawMerchants.toDF(), countValid, rejectedMerchants)
      summaryRows = summaryRows :+ merchantsSummary
    } catch {
      case ex: Exception => println(s" Erreur lors du traitement des [Commerçants] : ${ex.getMessage}")
    }

    // =========================================================================
    // 5. CONSOLIDATION DU RAPPORT GLOBAL DE SYNTHÈSE (EXPORT CSV)
    // =========================================================================
    if (summaryRows.nonEmpty) {
      val qualityReport = summaryRows.reduce(_ union _)

      println("\n " + "-"*60)
      println(" TALEAU DE SYNTHÈSE DE LA QUALITÉ DES DONNÉES (CONSOLIDÉ) :")
      println(" " + "-"*60)
      qualityReport.show(truncate = false)

      // Matérialisation finale du rapport consolidé en format plat CSV
      val csvReportPath = s"${outputBaseFolder}quality_report_summary"
      println(s" Sauvegarde du Bilan Qualité Global (format CSV) dans : $csvReportPath")
      qualityReport.coalesce(1).write
        .option("header", "true")
        .mode("overwrite")
        .csv(csvReportPath)
    } else {
      println("\n Aucun rapport de synthèse ni échantillon physique n'a pu être produit.")
    }

    println("\n" + "="*75)
    println("=== SPARK : PIPELINE TERMINE ET ENREGISTRE AVEC SUCCES ===")
    println("="*75 + "\n")

    spark.stop()
  }
}
