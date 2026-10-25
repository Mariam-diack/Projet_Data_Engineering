scalaVersion := "3.9.0"

name := "EcommerceAnalytics"

// On force sbt à charger les versions Spark de Scala 2.13 qui fonctionnent parfaitement sous Scala 3
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.5.1").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql"  % "3.5.1").cross(CrossVersion.for3Use2_13),
  "com.typesafe"       % "config"     % "1.4.3"
)
// Spécifier la classe principale
Compile / mainClass := Some("com.ecommerce.analytics.DataIngestion")


// Nom du fichier JAR généré
assembly / assemblyJarName := "ecommerce-analytics-assembly-1.0.jar"

// Stratégie de fusion pour éviter les erreurs de doublons
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}
