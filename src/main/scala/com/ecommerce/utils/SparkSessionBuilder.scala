package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

/** Construit la SparkSession à partir de application.conf. */
object SparkSessionBuilder {

  def build(): SparkSession =
    SparkSession.builder()
      .appName(ConfigLoader.appName)
      .master(ConfigLoader.sparkMaster)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions)
      .getOrCreate()
}