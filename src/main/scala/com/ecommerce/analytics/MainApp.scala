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

    println("\n" + "="*60)
    println("=== SPARK : DÉBUT DE L'AFFICHAGE DES DATASETS ===")
    println("="*60 + "\n")

    // --- 1. LECTURE DES TRANSACTIONS ---
    try {
      val transactions = ingestion.readTransactions()
      println(s" TRANSACTIONS : ${transactions.count()} lignes trouvées.")
      transactions.show(5)
    } catch {
      case e: Exception => println(s" Erreur [Transactions] : ${e.getMessage}")
    }

    // --- 2. LECTURE DES UTILISATEURS ---
    try {
      val users = ingestion.readUsers()
      println(s"\n UTILISATEURS : ${users.count()} lignes trouvées.")
      users.show(5)
    } catch {
      case e: Exception => println(s" Erreur [Utilisateurs] : ${e.getMessage}")
    }

    // --- 3. LECTURE DES PRODUITS ---
    try {
      val products = ingestion.readProducts()
      println(s"\n PRODUITS : ${products.count()} lignes trouvées.")
      products.show(5)
    } catch {
      case e: Exception => println(s" Erreur [Produits] : ${e.getMessage}")
    }

    // --- 4. LECTURE DES COMMERÇANTS ---
    try {
      val merchants = ingestion.readMerchants()
      println(s"\n COMMERÇANTS : ${merchants.count()} lignes trouvées.")
      merchants.show(5)
    } catch {
      case e: Exception => println(s" Erreur [Commerçants] : ${e.getMessage}")
    }

    println("\n" + "="*60)
    println("=== SPARK : FIN DE L'AFFICHAGE ===")
    println("="*60 + "\n")

    spark.stop()
  }
}
