package com.gianRock.spark.dbscan

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.mllib.util.Loader
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.{Loader, MLUtils, Saveable}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.mllib.util.Loader




case class Assignment(id: Long, cluster: Long)

/**
  * Created by rocco on 25/03/16.
  */
/**
  *
  * @param eps the radium  of the neighborhood ( the max distance)
  * @param minPts the min number of points, that  point should have
  *               in order to be considered Core-object
  */
class DbscanSpark(val eps:Double, val minPts:Int) {




  def run(sparkContext: SparkContext,distances:RDD[(Long, Long, Double)]):DbscanModel={
    val neighCouples=
    distances.filter{
      case(ida,idb,distance)=>distance<=eps
    }.flatMap{
     case(ida,idb,_)=>List((ida,idb),(idb,ida))
    }

    ???
  }


}

/**
  *
  * @param assignments this rdd contains for each id the correspondent cluster id
  *
  * @param eps
  * @param minPts
  */
class DbscanModel(val assignments: RDD[Assignment],val eps:Double,val minPts:Int)
  extends Saveable with Serializable {
  override def save(sc: SparkContext, path: String): Unit = {
    DbscanModel.SaveLoadV1_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"
}


object DbscanModel {
  object SaveLoadV1_0 {


    private val thisFormatVersion = "1.0"

    private[clustering]
    val thisClassName = "com.gianRock.spark.dbscan.DbsanModel"

    def save(sc: SparkContext, model: DbscanModel, path: String): Unit = {

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ ("eps" -> model.eps) ~ ("minPts" -> model.minPts)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path) )
      model.assignments.map(_.productIterator.mkString(",")).saveAsTextFile(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): DbscanModel = {
      implicit val formats = DefaultFormats

      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val eps = (metadata \ "eps").extract[Double]
      val minPts = (metadata \ "minPts").extract[Int]
      ???

      ///new DbscanModel()
    }
  }




}


private[dbscan] object Loader {

  /** Returns URI for path/data using the Hadoop filesystem */
  def dataPath(path: String): String = new Path(path, "data").toUri.toString

  /** Returns URI for path/metadata using the Hadoop filesystem */
  def metadataPath(path: String): String = new Path(path, "metadata").toUri.toString



  /**
    * Load metadata from the given path.
    *
    * @return (class name, version, metadata)
    */
  def loadMetadata(sc: SparkContext, path: String): (String, String, JValue) = {
    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(metadataPath(path)).first())
    val clazz = (metadata \ "class").extract[String]
    val version = (metadata \ "version").extract[String]
    (clazz, version, metadata)
  }
}