package com.ecommerce.models

case class User(
                 user_id: String,
                 age: Option[Long],
                 annual_income: Option[Double],
                 city: String,
                 customer_segment: String,
                 preferred_categories: Option[Seq[String]],
                 registration_date: String
               )

