package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Main {
  def main(args: Array[String]): Unit = {

    // Fix Hadoop et winutils pour l'exécution locale sous Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .master(ConfigLoader.sparkMaster)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions)
      .getOrCreate()

    // Masque les logs internes au profit de nos messages métiers
    spark.sparkContext.setLogLevel("WARN")

    val ingestion = new DataIngestion(spark)
    val validation = new DataValidation()

    println("\n" + "="*70)
    println("=== SPARK : PIPELINE DE QUALITÉ, GESTION DES ERREURS ET EXPORT ===")
    println("="*70 + "\n")

    // Variables pour centraliser les lignes de résumé en cas de succès
    var summaryRows = Seq.empty[DataFrame]

    // =========================================================================
    // 1. CHARGEMENT ET VALIDATION : TRANSACTIONS (CSV)
    // =========================================================================
    try {
      val rawTx = ingestion.readTransactions()
      val countRead = rawTx.count() // Déclenchement de l'action de lecture 🔍

      val (validTx, rejectedTx) = validation.validateTransactions(rawTx)
      val countValid = validTx.count()

      println(s" TRANSACTIONS | Lignes lues avant validation : $countRead | Lignes valides après validation : $countValid")

      val txSummary = validation.getDatasetSummary(spark, "transactions", rawTx.toDF(), countValid, rejectedTx)
      summaryRows = summaryRows :+ txSummary
    } catch {
      case ex: org.apache.spark.sql.AnalysisException =>
        println(s" Erreur de structure / Schéma sur [Transactions] : ${ex.getMessage}")
      case ex: java.io.FileNotFoundException =>
        println(s" Fichier [Transactions] introuvable au chemin indiqué : ${ex.getMessage}")
      case ex: Exception =>
        println(s" Erreur inattendue lors de la lecture des [Transactions] : ${ex.getMessage}")
    }

    // =========================================================================
    // 2. CHARGEMENT ET VALIDATION : UTILISATEURS (JSON)
    // =========================================================================
    try {
      val rawUsers = ingestion.readUsers()
      val countRead = rawUsers.count()

      val (validUsers, rejectedUsers) = validation.validateUsers(rawUsers)
      val countValid = validUsers.count()

      println(s" UTILISATEURS | Lignes lues avant validation : $countRead | Lignes valides après validation : $countValid")

      val usersSummary = validation.getDatasetSummary(spark, "users", rawUsers.toDF(), countValid, rejectedUsers)
      summaryRows = summaryRows :+ usersSummary
    } catch {
      case ex: org.apache.spark.sql.AnalysisException =>
        println(s" Erreur de structure / Schéma sur [Utilisateurs] : ${ex.getMessage}")
      case ex: java.io.FileNotFoundException =>
        println(s" Fichier [Utilisateurs] introuvable au chemin indiqué : ${ex.getMessage}")
      case ex: Exception =>
        println(s" Erreur inattendue lors de la lecture des [Utilisateurs] : ${ex.getMessage}")
    }

    // =========================================================================
    // 3. CHARGEMENT ET VALIDATION : PRODUITS (PARQUET)
    // =========================================================================
    try {
      val rawProducts = ingestion.readProducts()
      val countRead = rawProducts.count()

      val (validProducts, rejectedProducts) = validation.validateProducts(rawProducts)
      val countValid = validProducts.count()

      println(s" PRODUITS     | Lignes lues avant validation : $countRead | Lignes valides après validation : $countValid")

      val productsSummary = validation.getDatasetSummary(spark, "products", rawProducts.toDF(), countValid, rejectedProducts)
      summaryRows = summaryRows :+ productsSummary
    } catch {
      case ex: org.apache.spark.sql.AnalysisException =>
        println(s" Erreur de structure / Schéma sur [Produits] : ${ex.getMessage}")
      case ex: java.io.FileNotFoundException =>
        println(s" Fichier [Produits] introuvable au chemin indiqué : ${ex.getMessage}")
      case ex: Exception =>
        println(s" Erreur inattendue lors de la lecture des [Produits] : ${ex.getMessage}")
    }

    // =========================================================================
    // 4. CHARGEMENT ET VALIDATION : COMMERÇANTS (CSV InferSchema)
    // =========================================================================
    try {
      val rawMerchants = ingestion.readMerchants()
      val countRead = rawMerchants.count()

      val (validMerchants, rejectedMerchants) = validation.validateMerchants(rawMerchants)
      val countValid = validMerchants.count()

      println(s" COMMERÇANTS  | Lignes lues avant validation : $countRead | Lignes valides après validation : $countValid")

      val merchantsSummary = validation.getDatasetSummary(spark, "merchants", rawMerchants.toDF(), countValid, rejectedMerchants)
      summaryRows = summaryRows :+ merchantsSummary
    } catch {
      case ex: org.apache.spark.sql.AnalysisException =>
        println(s" Erreur de structure / Schéma sur [Commerçants] : ${ex.getMessage}")
      case ex: java.io.FileNotFoundException =>
        println(s" Fichier [Commerçants] introuvable au chemin indiqué : ${ex.getMessage}")
      case ex: Exception =>
        println(s" Erreur inattendue lors de la lecture des [Commerçants] : ${ex.getMessage}")
    }

    // =========================================================================
    // 5. GÉNÉRATION ET EXPORT DU BILAN SYNTHÉTIQUE GLOBAL
    // =========================================================================
    if (summaryRows.nonEmpty) {
      // Fusion dynamique des lignes de résumés générées avec succès
      val qualityReport = summaryRows.reduce(_ union _)

      println("\n📊 " + "-"*60)
      println(" TABLEAU DE SYNTHÈSE DE LA QUALITÉ DES DONNÉES (CONSOLIDÉ) :")
      println(" " + "-"*60)
      qualityReport.show(truncate = false)

      val exportPath = s"${ConfigLoader.outputPath}quality_report_summary"
      println(s" Sauvegarde du rapport final (.csv) dans : $exportPath")

      qualityReport.coalesce(1).write
        .option("header", "true")
        .mode("overwrite")
        .csv(exportPath)
    } else {
      println("\n Aucun rapport de synthèse généré : tous les chargements de fichiers ont échoué.")
    }

    println("\n" + "="*70)
    println("=== SPARK : FIN DE LA SOUPOUDREUSE DE QUALITÉ ===")
    println("="*70 + "\n")

    spark.stop()
  }
}
