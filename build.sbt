name := "EcommerceAnalytics"

version := "1.0"

scalaVersion := "2.13.13"  // Assurez-vous que la version de Scala correspond aussi

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.2.0" % "provided",
  "org.apache.spark" %% "spark-sql"  % "4.2.0" % "provided",
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
