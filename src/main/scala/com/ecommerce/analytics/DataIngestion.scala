
package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

class DataIngestion(spark: SparkSession) {

  import spark.implicits._

  // Récupérer le chemin d'un fichier depuis application.conf
  private def inputPath(dataset: String): String = dataset match {
    case "transactions" => ConfigLoader.transactionsPath
    case "users" => ConfigLoader.usersPath
    case "products" => ConfigLoader.productsPath
    case "merchants" => ConfigLoader.merchantsPath
    case _ => ""
  }

  // 1. Lecture des transactions CSV
  def readTransactions(): Dataset[Transaction] = {

    val transactionSchema = StructType(Seq(
      StructField("transaction_id", StringType, true),
      StructField("user_id", StringType, true),
      StructField("product_id", StringType, true),
      StructField("merchant_id", StringType, true),
      StructField("amount", DoubleType, true),
      StructField("timestamp", StringType, true),
      StructField("location", StringType, true),
      StructField("payment_method", StringType, true),
      StructField("category", StringType, true)
    ))

    spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(transactionSchema)
      .csv(inputPath("transactions"))
      .as[Transaction]
  }

  // 2. Lecture des utilisateurs JSON
  def readUsers(): Dataset[User] = {

    spark.read
      .option("multiLine", "false")
      .json(inputPath("users"))
      .as[User]
  }

  // 3. Lecture des produits Parquet
  def readProducts(): Dataset[Product] = {

    spark.read
      .parquet(inputPath("products"))
      .as[Product]
  }

  // 4. Lecture des commerçants CSV
  def readMerchants(): Dataset[Merchant] = {

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath("merchants"))
      .as[Merchant]
  }
}

