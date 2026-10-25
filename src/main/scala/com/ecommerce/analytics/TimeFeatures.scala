package com.ecommerce.analytics

import java.time.format.{DateTimeFormatter, ResolverStyle, TextStyle}
import java.time.{DayOfWeek, LocalDateTime}
import java.util.Locale

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

// Résultat de l'extraction : les six infos qu'on tire du timestamp
case class TimeFeaturesResult(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int
)

object TimeFeatures {

  // un timestamp valide fait exactement 14 chiffres (yyyyMMddHHmmss)
  private val Pattern = "^\\d{14}$".r

  // mode STRICT : une date impossible (le 30 février par exemple) est refusée
  // au lieu d'être corrigée toute seule par Java
  private val Formatter =
    DateTimeFormatter.ofPattern("uuuuMMddHHmmss").withResolverStyle(ResolverStyle.STRICT)

  // Renvoie None si le timestamp est null, vide ou invalide, comme ça le job ne plante jamais
  def extract(timestamp: String): Option[TimeFeaturesResult] =
    Option(timestamp).map(_.trim).filter(Pattern.matches(_)).flatMap { ts =>
      Try(LocalDateTime.parse(ts, Formatter)).toOption.map { dt =>
        val hour = dt.getHour
        val day  = dt.getDayOfWeek
        TimeFeaturesResult(
          hour             = hour,
          day_of_week      = day.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          month            = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          is_weekend       = if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) 1 else 0,
          day_period       = dayPeriod(hour),
          is_working_hours = if (hour >= 9 && hour < 17) 1 else 0
        )
      }
    }

  // Morning de 6h à 12h, Afternoon de 12h à 18h, Evening de 18h à 22h.
  // Tout le reste c'est Night, donc aussi de minuit à 6h (le sujet ne le précise pas).
  private def dayPeriod(hour: Int): String =
    if (hour >= 6 && hour < 12) "Morning"
    else if (hour >= 12 && hour < 18) "Afternoon"
    else if (hour >= 18 && hour < 22) "Evening"
    else "Night"

  // L'UDF à appliquer sur la colonne timestamp. Un timestamp invalide donne null.
  val extractTimeFeatures: UserDefinedFunction = udf((timestamp: String) => extract(timestamp))
}
