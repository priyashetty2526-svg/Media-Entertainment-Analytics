ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Media_Entertainment_Data_Analytics",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.7"
    )
  )
