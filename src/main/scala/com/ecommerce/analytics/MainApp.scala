package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import com.ecommerce.utils.ConfigLoader

object Main {
  def main(args: Array[String]): Unit = {

    // Configurer le chemin Hadoop pour Windows avant d'initialiser Spark
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    
    // Initialisation dynamique via votre ConfigLoader
    val spark = SparkSession.builder()
      .appName(ConfigLoader.appName)
      .master(ConfigLoader.sparkMaster)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val ingestion = new DataIngestion(spark)
    val validation = new DataValidation() // classe de nettoyage 🌟

    println("\n=========================================")
    println("=== SPARK : PIPELINE DE VALIDATION ACTIVÉ ===")
    println("=========================================\n")

    // 1. Ingestion + Validation des Transactions
    val rawTx = ingestion.readTransactions()
    val validTx = validation.validateTransactions(rawTx) // Filtrage actif
    println(s"📊 TRANSACTIONS | Brut : ${rawTx.count()} -> Valide : ${validTx.count()}")
    validTx.show(5)

    // 2. Ingestion + Validation des Utilisateurs
    val rawUsers = ingestion.readUsers()
    val validUsers = validation.validateUsers(rawUsers) // Filtrage actif
    println(s"\n📊 UTILISATEURS | Brut : ${rawUsers.count()} -> Valide : ${validUsers.count()}")
    validUsers.show(5)

    // 3. Ingestion + Validation des Produits
    val rawProducts = ingestion.readProducts()
    val validProducts = validation.validateProducts(rawProducts) // Filtrage actif
    println(s"\n📊 PRODUITS     | Brut : ${rawProducts.count()} -> Valide : ${validProducts.count()}")
    validProducts.show(5)

    // 4. Ingestion + Validation des Commerçants
    val rawMerchants = ingestion.readMerchants()
    val validMerchants = validation.validateMerchants(rawMerchants) // Filtrage actif
    println(s"\n📊 COMMERÇANTS  | Brut : ${rawMerchants.count()} -> Valide : ${validMerchants.count()}")
    validMerchants.show(5)

    spark.stop()
  }
}
