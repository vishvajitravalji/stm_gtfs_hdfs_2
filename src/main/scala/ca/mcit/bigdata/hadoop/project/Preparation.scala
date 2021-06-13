package ca.mcit.bigdata.hadoop.project

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, FileUtil, Path}

trait Preparation {

  val conf = new Configuration()

  val hadoopConfDir = "C:\\Users\\Vish\\Desktop\\hadoop"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  val fs: FileSystem = FileSystem.get(conf)

  fs.delete(new Path("/user/bdss2001/vish1/course3"))
  fs.copyFromLocalFile(new Path("Data\\trips.txt"), new Path("/user/bdss2001/vish1/stm"))
  fs.copyFromLocalFile(new Path("Data\\routes.txt"), new Path("/user/bdss2001/vish1/stm"))
  fs.copyFromLocalFile(new Path("Data\\calendar.txt"), new Path("/user/bdss2001/vish1/stm"))


}
