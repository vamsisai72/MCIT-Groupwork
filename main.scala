package ca.mcit.Hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.IOException

object main extends App {

  val conf = new Configuration()
  conf.addResource(new Path("/home/marinda/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/marinda/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))

  val fs: FileSystem = FileSystem.get(conf)
  val groupPath: Path = new Path("/user/fall2019")
  val myPath: Path = new Path("/user/fall2019/marinda")

  println("URI : "+ fs.getUri)
  println("Working dir : "+ fs.getWorkingDirectory)

  if (fs.isDirectory(myPath)) {
    println(s"I found my folder $myPath, and now I am going to delete it")
 //   fs.delete(myPath, true)
  } else {
    println(s"Folder $myPath does not exist")
  }

  if (fs.isDirectory(myPath)) {
    println(s"My folder $myPath still exists, so the delete did not work")
  } else {
    println(s"Folder $myPath deleted successfully")
  }

  try {
    fs.mkdirs(myPath)
  }
  catch {
    case _ => println("Could not create subdir ../marinda")
  }

  if (fs.isDirectory(myPath)) {
    println(s"My folder $myPath created successfully")
  } else {
    println(s"My folder $myPath was not created")
  }


  // Using try-catch to manage errors and print success msgs
  try{
    fs.mkdirs(new Path(myPath+"/stm"))
  }
  catch {
    case _ => println("Could not create subdir ../stm")
  }


  fs.copyFromLocalFile(new Path("/home/marinda/Documents/MCIT/Data/stops.txt"),
    new Path("/user/fall2019/marinda/stm/"))
  fs.copyFromLocalFile(new Path("/home/marinda/Documents/MCIT/Data/stops.txt"),
    new Path("/user/fall2019/marinda/stm/stops2.txt"))
  fs.rename(new Path("/user/fall2019/marinda/stm/stops.txt"),
    new Path("/user/fall2019/marinda/stm/stops.csv"))



  /*
  println("Files in groupPath")
  println("==================================")
  fs
    .listStatus(groupPath)
    .foreach(println)

  println("Files in myPath")
  println("==================================")
  fs
    .listStatus(myPath)
    .foreach(println)


  if (fs.isDirectory(myPath))
    println("Delete did not work")
  else
    println("Folder deleted")

  println("Files in myPath")
  println("==================================")
  fs
    .listStatus(myPath)
    .foreach(println)

  */
}
