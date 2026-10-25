scalaVersion := "2.13.14"

name := "EcommerceAnalytics"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql"  % "3.5.1",
  "com.typesafe"      % "config"     % "1.4.3"
)


assembly / mainClass := Some("com.ecommerce.analytics.MainApp")
assembly / assemblyJarName := "EcommerceAnalytics.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case _                             => MergeStrategy.first
}
