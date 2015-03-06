name := "DHIN"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % "1.2.0",
  "org.apache.spark" %% "spark-graphx"  % "1.2.0")

resolvers += Resolver.sonatypeRepo("snapshots")
