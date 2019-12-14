package com.sample
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}
import org.apache.hadoop.fs._
import java.net.URI
import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException

import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.types.{IntegerType,StringType, StructField, StructType}

import scala.Array
import scala.collection.mutable.ListBuffer

object LogProcess{

  val requiredFields = Seq("customer_id","created_at","product_id","request_id","random1","random2","random3","random4")

  val jsonSchema = StructType(Array(
    StructField("customer_id",IntegerType,true),
    StructField("created_at",StringType,true),
    StructField("product_id",IntegerType,true),
    StructField("request_id",IntegerType,true),
    StructField("random1",StringType,true),
    StructField("random2",StringType,true),
    StructField("random3",StringType,true),
    StructField("random4",StringType,true)
  )
  )
}
object LogProcessing extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val ss = SparkSession.builder().appName("Log Procesing").master("local[*]").getOrCreate()
  val path:String = "H:\\spark\\logs";
  val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
  val fStatus = LogProcessing.fetchLogFilesToProcess(fs,new Path(path))

  //println(fStatus.size)

  import com.sample.DataFrameColumnChecker
  // read the files
  try {
    val data = fStatus.map(x => x.getPath.toString).map(path => ss.read.json(path))
      //.filter(x => DataFrameColumnChecker.validateColumns(x,requiredFields) )
      .filter(x => DataFrameColumnChecker(x, LogProcess.requiredFields).validateColumns)
      .map(df => df.select("customer_id", "product_id", "request_id", "created_at"))
      .reduce[DataFrame](_ union _)

    data.write.csv("H:\\spark\\logs\\out")

    appendFilesWithCompleted(fs,fStatus)

  }catch{
    case ex:MissingDataFrameColumnException => println(ex.msg)
  }
  def fetchLogFilesToProcess(fileSystem:FileSystem, fpath:Path):Array[FileStatus]={
    fs.listStatus(fpath, new PathFilter {
      override def accept(fpath: Path): Boolean =
        return fs.isFile(fpath) && !path.endsWith("COMPLETED")
    })
  }
  def appendFilesWithCompleted(fs:FileSystem, fileStatus: Array[FileStatus]):Unit ={
    val fileNames = fileStatus.map(x => x.getPath)
      .foreach(path => {
        try{
          fs.rename(path,new Path(path.toString+".COMPLETE"))
        }
        catch{
          case e:IOException => println(e.getMessage)
        }finally{
          println("Completed")
        }
      })
  }

}
