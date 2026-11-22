ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "io.github.vaibhav4046"

val sparkVersion = "3.5.8"

// JDK 17 needs these opens for Spark's off-heap / reflection code paths.
val sparkJavaOpts = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "nyc-rideshare-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    // `sbt run` puts the Provided Spark jars back on the classpath; spark-submit supplies them in a cluster.
    Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
    run / fork := true,
    run / javaOptions ++= sparkJavaOpts ++ Seq("-Duser.timezone=UTC", s"-Xmx${sys.env.getOrElse("DRIVER_MEMORY", "6g")}"),
    run / connectInput := false,
    // Tests run inside the sbt JVM; .jvmopts supplies the same --add-opens flags and UTC timezone.
    Test / parallelExecution := false
  )
