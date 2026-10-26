package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.Dataset

class DataValidation {

  /**
   * Transactions : amount > 0 et timestamp comportant 14 caractères.
   */
  def validateTransactions(transactions: Dataset[Transaction]): Dataset[Transaction] = {
    transactions.filter(tx =>
      tx.amount.getOrElse(0.0) > ConfigLoader.minAmount &&
        tx.timestamp != null &&
        tx.timestamp.length == ConfigLoader.timestampLength
    )
  }

  /**
   * Users : age compris entre 16 et 100, et annual_income > 0.
   */
  def validateUsers(users: Dataset[User]): Dataset[User] = {
    users.filter(u =>
      // u.age est un Option[Long], on applique .getOrElse(0L)
      u.age.getOrElse(0L) >= ConfigLoader.minAge &&
        u.age.getOrElse(0L) <= ConfigLoader.maxAge &&
        // u.annual_income est un Option[Double], on applique .getOrElse(0.0)
        u.annual_income.getOrElse(0.0) > ConfigLoader.minIncome
    )
  }

  /**
   * Products : price > 0 et rating compris entre 1 et 5.
   */
  def validateProducts(products: Dataset[Product]): Dataset[Product] = {
    products.filter(p =>
      p.price.getOrElse(0.0) > ConfigLoader.minPrice &&
        p.rating.getOrElse(0.0) >= ConfigLoader.minRating &&
        p.rating.getOrElse(0.0) <= ConfigLoader.maxRating
    )
  }

  /**
   * Merchants : commission_rate compris entre 0 et 1.
   */
  def validateMerchants(merchants: Dataset[Merchant]): Dataset[Merchant] = {
    merchants.filter(m =>
      // m.commission_rate est un Option[Double], on applique .getOrElse(0.0)
      m.commission_rate.getOrElse(0.0) >= ConfigLoader.minCommission &&
        m.commission_rate.getOrElse(0.0) <= ConfigLoader.maxCommission
    )
  }
}
