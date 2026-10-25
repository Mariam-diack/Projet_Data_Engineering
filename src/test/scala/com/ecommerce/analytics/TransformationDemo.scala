package com.ecommerce.analytics

import com.ecommerce.utils.{ConfigLoader, SparkSessionBuilder}
import org.apache.spark.sql.functions._

/**
 * Démonstration de la Partie 3 (Membre B), indépendante de MainApp.
 * Lancement : sbt "Test/runMain com.ecommerce.analytics.TransformationDemo"
 */
object TransformationDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBuilder.build()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val transactions = spark.read.option("header", "true").csv(ConfigLoader.transactionsPath)
        .withColumn("amount", col("amount").cast("double"))
      val users     = spark.read.json(ConfigLoader.usersPath)
      val products  = spark.read.parquet(ConfigLoader.productsPath)
      val merchants = spark.read.option("header", "true").option("inferSchema", "true").csv(ConfigLoader.merchantsPath)

      println("\n=== Q3.1 : UDF extractTimeFeatures ===")
      transactions.withColumn("tf", TimeFeatures.extractTimeFeatures(col("timestamp")))
        .select("transaction_id", "timestamp", "tf.*").show(5, truncate = false)

      println("=== Timestamps invalides (tf null) ===")
      val invalid = transactions.withColumn("tf", TimeFeatures.extractTimeFeatures(col("timestamp")))
        .filter(col("tf").isNull)
      println(s"Lignes invalides : ${invalid.count()} sur ${transactions.count()}")
      invalid.select("transaction_id", "timestamp").show(5, truncate = false)

      println("=== Q3.2 : enrichTransactionData ===")
      val transformation = new DataTransformation()
      val enriched = transformation.enrichTransactionData(transactions, users, products, merchants)
      println(s"Lignes avant jointures : ${transactions.count()} | après : ${enriched.count()}")
      println(s"Orphelins : utilisateur=${enriched.filter(col("age").isNull && col("user_id").isNotNull).count()}, " +
        s"produit=${enriched.filter(col("product_name").isNull).count()}, " +
        s"marchand=${enriched.filter(col("merchant_name").isNull).count()}")
      enriched.groupBy("age_group").count().orderBy("age_group").show()

      println("=== Q3.3 : addBehaviorFeatures (historique de U00001) ===")
      val behavior = transformation.addBehaviorFeatures(enriched)
      behavior.filter(col("user_id") === "U00001").orderBy("transaction_ts")
        .select("transaction_id", "timestamp", "amount", "user_transaction_rank", "user_total_transactions",
          "amount_7d", "active_days_7d", "active_user", "days_since_previous_purchase")
        .show(20, truncate = false)
      behavior.groupBy("active_user").count().show()

      println("=== Q3.4 (bonus) : transactions suspectes ===")
      val flagged = transformation.addSuspiciousFlag(behavior).cache()
      println(s"Lignes : ${flagged.count()} | répartition du score :")
      flagged.groupBy("suspicion_score").count().orderBy("suspicion_score").show()
      transformation.showSuspiciousSummary(flagged)
    } finally {
      spark.stop()
    }
  }
}
