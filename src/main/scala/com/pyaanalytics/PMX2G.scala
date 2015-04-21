/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pyaanalytics

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scopt.OptionParser
import scala.xml._

class VertexProperty()
case class AuthorProperty(name: String) extends VertexProperty
case class PaperProperty(pmid: Int) extends VertexProperty

case class Vertex(vid: VertexId, prop: VertexProperty)

object PMX2G {

  case class PMX2GConfig(xmlFile: String = "",
                         vertexPath: String = "../full_vertices",
                         edgePath: String = "../full_edges",
                         sparkMaster: String = "master[64]"
  )

  def hash64(string: String): Long = {
    string.map(_.toLong).foldLeft(1125899906842597L)((h: Long, c: Long) => 31 * h + c)
  }

  def processRecord(recString: String): Seq[(Vertex, Vertex)] = {
    try {
      val rec = XML.loadString(recString)
      val pmid = (rec \\ "MedlineCitation" \ "PMID").text
      val thisVert = Vertex(hash64(pmid), PaperProperty(pmid.toInt))
      val abstractText = rec \\ "MedlineCitation" \ "Article" \ "Abstract" \ "AbstractText"
      if (abstractText.length == 0 || pmid.toInt == 0) {return Seq()}
      val firstNames = (rec \\ "AuthorList" \ "Author" \ "ForeName") map (_.text.charAt(0).toLower)
      val lastNames = (rec \\ "AuthorList" \ "Author" \ "LastName") map (_.text.replaceAll("""\s+$""", "").toLowerCase())
      val authors = firstNames
        .zip(lastNames)
        .map{case (f, l) => f + " " + l}
        .map{x => Vertex(hash64(x), AuthorProperty(x))}
      val citations = (rec \\ "CommentCorrectionsList" \ "CommentsCorrections" \ "PMID")
        .map{x => Vertex(hash64(x.text), PaperProperty(x.text.toInt))}
      authors.map((thisVert, _)) ++ citations.map((thisVert, _))
    } catch {
      case e: Exception => Seq()
    }
  }

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[PMX2GConfig]("PMX2G") {


      arg[String]("xmlFile") valueName("xmlFile") action {
        (x, c) => c.copy(xmlFile = x)
      }

      arg[String]("vertexPath") valueName("vertexPath") action {
        (x, c) => c.copy(vertexPath = x)
      }

      arg[String]("edgePath") valueName("edgePath") action {
        (x, c) => c.copy(edgePath = x)
      }
      arg[String]("sparkMaster") valueName("sparkMaster") action {
        (x, c) => c.copy(sparkMaster = x)
      }
    }

    parser.parse(args, PMX2GConfig()) match {
      case Some(config) => {
        val sparkConf = new SparkConf()
          .setAppName("XML 2 Graph")
          .setMaster(config.sparkMaster)
          .set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")
          .set("spark.executor.memory", "200g")
          .set("spark.driver.memory", "200g")

        sparkConf.registerKryoClasses(Array(classOf[Vertex],
                                            classOf[VertexProperty],
                                            classOf[AuthorProperty],
                                            classOf[PaperProperty]))
        val sc = new SparkContext(sparkConf)

        val nodeRDD = sc.textFile(config.xmlFile) flatMap processRecord
        nodeRDD.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
        val vertices = nodeRDD flatMap {case (p, v) => Seq(p, v)}
        vertices.saveAsObjectFile(config.vertexPath)
        val edges = nodeRDD map {case (p, v) => Edge(p.vid, v.vid, 0)}
        edges.saveAsObjectFile(config.edgePath)
        sc.stop()
      } case None => {
        System.exit(1)
      }
    }
  }
}
