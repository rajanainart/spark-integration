name := "spark-import-job"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

resourceDirectory in Compile := baseDirectory.value / "src/resources"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion,
  "org.apache.spark" %% "spark-sql"       % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10-assembly" % sparkVersion,
  "com.memsql" % "memsql-connector_2.11" % "2.0.7",
  "mysql"      % "mysql-connector-java"  % "8.0.11",
  "com.oracle" % "ojdbc7"                % "12.1.0.2",
  "org.postgresql" % "postgresql" % "42.2.6"

  //"org.apache.spark" %% "spark-hive"      % sparkVersion,
  //"org.apache.spark" %% "spark-mllib"     % sparkVersion,
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "services" :: _ =>  MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}