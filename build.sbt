scalaVersion := "3.9.0"

name := "EcommerceAnalytics"

// On force sbt à charger les versions Spark de Scala 2.13 qui fonctionnent parfaitement sous Scala 3
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.5.1").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql"  % "3.5.1").cross(CrossVersion.for3Use2_13),
  "com.typesafe"       % "config"     % "1.4.3"
)
