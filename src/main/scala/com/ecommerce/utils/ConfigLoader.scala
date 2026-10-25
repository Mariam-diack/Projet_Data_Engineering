package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

/** Accès à application.conf. Chaque lecture accepte une valeur par défaut si la clé est absente. */
object ConfigLoader {

  lazy val config: Config = ConfigFactory.load().getConfig("app")

  def getString(path: String, default: String): String =
    if (config.hasPath(path)) config.getString(path) else default

  def getInt(path: String, default: Int): Int =
    if (config.hasPath(path)) config.getInt(path) else default

  def getDouble(path: String, default: Double): Double =
    if (config.hasPath(path)) config.getDouble(path) else default

  def getBoolean(path: String, default: Boolean): Boolean =
    if (config.hasPath(path)) config.getBoolean(path) else default

  // Chemins des données
  def transactionsPath: String = getString("data.input.transactions", "src/main/resources/data/transactions.csv")
  def usersPath: String        = getString("data.input.users", "src/main/resources/data/users.json")
  def productsPath: String     = getString("data.input.products", "src/main/resources/data/products.parquet")
  def merchantsPath: String    = getString("data.input.merchants", "src/main/resources/data/merchants.csv")
  def outputPath: String       = getString("data.output.path", "output/")

  // Spark et optimisations
  def appName: String           = getString("name", "EcommerceAnalytics")
  def sparkMaster: String       = getString("spark.master", "local[*]")
  def shufflePartitions: Int    = getInt("spark.shuffle.partitions", 8)
  def enableCache: Boolean      = getBoolean("optimization.enable-cache", true)
  def enableBroadcast: Boolean  = getBoolean("optimization.enable-broadcast", true)

  // Seuils de validation
  def minAmount: Double        = getDouble("validation.transaction.min-amount", 0.0)
  def timestampLength: Int     = getInt("validation.transaction.timestamp-length", 14)
  def minAge: Int              = getInt("validation.user.min-age", 16)
  def maxAge: Int              = getInt("validation.user.max-age", 100)
  def minIncome: Double        = getDouble("validation.user.min-income", 0.0)
  def minPrice: Double         = getDouble("validation.product.min-price", 0.0)
  def minRating: Double        = getDouble("validation.product.min-rating", 1.0)
  def maxRating: Double        = getDouble("validation.product.max-rating", 5.0)
  def minCommission: Double    = getDouble("validation.merchant.min-commission", 0.0)
  def maxCommission: Double    = getDouble("validation.merchant.max-commission", 1.0)
}
