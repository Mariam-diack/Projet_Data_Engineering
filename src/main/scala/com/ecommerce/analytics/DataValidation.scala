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
      tx.amount > ConfigLoader.minAmount &&
        tx.timestamp != null &&
        tx.timestamp.length == ConfigLoader.timestampLength
    )
  }

  /**
   * Users : age compris entre 16 et 100, et annual_income > 0.
   */
  def validateUsers(users: Dataset[User]): Dataset[User] = {
    users.filter(u =>
      u.age >= ConfigLoader.minAge &&
        u.age <= ConfigLoader.maxAge &&
        u.income > ConfigLoader.minIncome
    )
  }

  /**
   * Products : price > 0 et rating compris entre 1 et 5.
   */
  def validateProducts(products: Dataset[Product]): Dataset[Product] = {
    products.filter(p =>
      p.price > ConfigLoader.minPrice &&
        p.rating >= ConfigLoader.minRating &&
        p.rating <= ConfigLoader.maxRating
    )
  }

  /**
   * Merchants : commission_rate compris entre 0 et 1.
   */
  def validateMerchants(merchants: Dataset[Merchant]): Dataset[Merchant] = {
    merchants.filter(m =>
      m.commission_rate >= ConfigLoader.minCommission &&
        m.commission_rate <= ConfigLoader.maxCommission
    )
  }
}
