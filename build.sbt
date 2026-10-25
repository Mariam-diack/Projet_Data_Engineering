name := "EcommerceAnalytics"

version := "1.0"

scalaVersion := "2.13.14"

libraryDependencies ++= Seq(
  // % Provided est obligatoire si vous déployez avec spark-submit sur cluster,
  // mais vous pouvez le retirer temporairement si vous exécutez uniquement via sbt run en local.
  "org.apache.spark" %% "spark-core" % "3.5.1" % Provided,
  "org.apache.spark" %% "spark-sql"  % "3.5.1" % Provided,
  "com.typesafe"      % "config"     % "1.4.3"
)

// Configure SBT pour exécuter MainApp par défaut en local
Compile / mainClass := Some("com.ecommerce.analytics.MainApp")

// Configuration du plugin sbt-assembly exigée par le correcteur
assembly / mainClass := Some("com.ecommerce.analytics.MainApp")
assembly / assemblyJarName := "EcommerceAnalytics.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    // TRÈS IMPORTANT : Permet de fusionner correctement les fichiers de configuration HOCON
  case "reference.conf"              => MergeStrategy.concat
  case "application.conf"            => MergeStrategy.concat
  case _                             => MergeStrategy.first
}
