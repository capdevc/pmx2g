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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scopt.OptionParser
import scala.math.random
import scala.collection.mutable._

case class RandomTextWriterConfig(output: String = "",
                                  minKey: Int = 5, maxKey: Int = 10,
                                  minValue: Int = 10, maxValue: Int = 100,
                                  mapNum: Int = 1,
                                  userName: String = "spark",
                                  megaBytesPerMap: Long = 1)

object RandomTextWriter {

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[RandomTextWriterConfig]("RandomTextWriter") {
      opt[Int]('k', "minKey") valueName("minKey") action {
        (x, c) => c.copy(minKey = x)
      }

      opt[Int]('K', "maxKey") valueName("maxKey") action {
        (x, c) => c.copy(maxKey = x)
      }

      opt[Int]('v', "minValue") valueName("minValue") action {
        (x, c) => c.copy(minValue = x)
      }

      opt[Int]('V', "maxValue") valueName("maxValue") action {
        (x, c) => c.copy(maxValue = x)
      }

      opt[Long]('b', "megaBytesPerMap [MB]") valueName("megaBytesPerMap") action {
        (x, c) => c.copy(megaBytesPerMap = x)
      }

      opt[Int]('n', "mapNum") valueName("mapNum") action {
        (x, c) => c.copy(mapNum = x)
      }

      opt[String]('u', "userName") valueName("userName") action {
        (x, c) => c.copy(userName = x)
      }

      arg[String]("output") valueName("output") action {
        (x, c) => c.copy(output = x)
      }
    }

    parser.parse(args, RandomTextWriterConfig()) map { config =>
      val wordsInKeyRange = config.maxKey - config.minKey
      val wordsInValueRange = config.maxValue - config.minValue

      val sparkConf = new SparkConf()
      val sc = new SparkContext(sparkConf)

      val randomTextWriter = new RandomTextWriter(sc, config.minKey, wordsInKeyRange,
        config.minValue, wordsInValueRange, config.mapNum, config.megaBytesPerMap, config.output)

      println("Total size: " + randomTextWriter.totalSize.value + " [byte]")

      sc.stop()

    } getOrElse {
      System.exit(1)
    }

  }
}

@SerialVersionUID(1L)
class RandomTextWriter(sc: SparkContext, minKey: Int, wordsInKeyRange: Int,
          minValue: Int, wordsInValueRange: Int, mapNum: Int, megaBytesPerMap: Long, output: String) 
          extends Serializable {

  val totalSize = sc.accumulator(0: Long)

  sc.parallelize(1 to mapNum, mapNum).flatMap { id =>
    val iter = new Iterator[(String, String)] {
      private var bytes: Long = megaBytesPerMap * 1024 * 1024
      def hasNext = bytes > 0
      def next = {
        if (bytes > 0) {
          val noWordsKey = if (wordsInKeyRange != 0) (minKey + random * wordsInKeyRange).toInt else 0
          val noWordsValue = if (wordsInValueRange != 0) (minValue + random * wordsInValueRange).toInt else 0

          val keyWords = generateSentence(noWordsKey)
          val valueWords = generateSentence(noWordsValue)

          bytes -= keyWords.getBytes("utf8").length + valueWords.getBytes("utf8").length
          totalSize += keyWords.getBytes("utf8").length + valueWords.getBytes("utf8").length

          (keyWords, valueWords)
        } else {
          error("next on empty iterator")
        }
      }
    }

    iter
  }.map { t =>
    t._1 + "\t" + t._2
  }.saveAsTextFile(output)

  def generateSentence(noWords: Int) = {
    val sentence = new StringBuilder
    val space = " "

    for (i <- 0 to noWords -1) {
      sentence ++= Words.words((random * Words.words.length).toInt)
      sentence ++= space
    }
    sentence.toString
  }


}

// vim: ft=scala tw=0 sw=2 ts=2 et
