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

package ca.uwaterloo.cs.bigdata2017w.project

import java.io.{ByteArrayInputStream, DataInputStream, ByteArrayOutputStream, DataOutputStream}

import _root_.io.bespin.scala.util.Tokenizer
import org.apache.hadoop.io.WritableUtils
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.graphx._

import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD


/**
 * Example Usage:
 * $ spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2017w.project.CompressGraph \
 *   target/bigdata2017w-0.1.0-SNAPSHOT.jar --input data/out.munmun_twitter_social \
 *   --output data/twitter_small
 * Expected compression ratio is about 8.0
 */
class CompressGraphConfig(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val localtest = opt[Boolean](descr = "verify correctness lcoally", required = false, default = Some(false) )
  verify()
}

object CompressGraph {
  val log = Logger.getLogger(getClass().getName())

  def encodeSeq( lst: Seq[Long] ): Array[Byte] = {
    val aos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(aos)

    WritableUtils.writeVInt(dos, lst.size)
    if (lst.size > 0) {
      var last: Long = 0

      lst.foreach( x => {
        WritableUtils.writeVInt(dos, (x - last).toInt)
        last = x
      })
    }

    aos.toByteArray
  }

  def decodeSeq( bytes: Array[Byte] ): Seq[Long] = {
    val dis = new DataInputStream(new ByteArrayInputStream(bytes))

    val sz = WritableUtils.readVInt(dis)

    var last: Long = 0
    var seq = new ListBuffer[Long]
    1 to sz foreach { _ => {
        val x = WritableUtils.readVInt(dis) + last
        seq += x
        last = x
      }
    }
    seq.toList
  }

  def compress( graph: Graph[Int, Int] ): RDD[(Long, Array[Byte])] = {
    /**
     * Now every line is a unique vertexId followd by a sorted list of its out-neighbours.
     * This reduces the text file size by about a half.
     */
    val graphStripes = graph.aggregateMessages[List[Long]](
      triplet => {
        triplet.sendToSrc(List(triplet.dstId))
      },

      (msg1, msg2) => (msg1 ++ msg2)
    )

    // Now compress the each list of out neighbours
    graphStripes.map( x => (x._1, encodeSeq(x._2.sorted)) )
  }

  def decompress( compressed: RDD[(Long, Array[Byte])] ): Graph[Int, Int] = {
    val edges: RDD[Edge[Int]] = compressed.flatMap( v_neigbours => {
      val srcId: Long = v_neigbours._1

      val neighbours: Seq[Long] = decodeSeq(v_neigbours._2)

      neighbours.map( dstId => Edge(srcId, dstId, 0) )
    })

    Graph.fromEdges(edges, 0)
  }

  def loadCompressedGraph( path: String, sc: SparkContext ): Graph[Int, Int] = {
    decompress(sc.objectFile( path ))
  }

  /**
   * For test on small cases only!
   */
  def graphEqual( graphA: Graph[Int, Int], graphB: Graph[Int, Int]): Boolean = {
    val edgesA: Array[(Long, Long)] = graphA.edges.map( e => (e.srcId, e.dstId) ).collect().sorted
    val edgesB: Array[(Long, Long)] = graphB.edges.map( e => (e.srcId, e.dstId) ).collect().sorted

    edgesA.deep == edgesB.deep
  }

  def main(argv: Array[String]): Unit = {

    val args = new CompressGraphConfig(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("Train Spam Classifier")
    val sc = new SparkContext(conf)

    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, args.input())

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val compressedGraph = compress(graph)
    compressedGraph.saveAsObjectFile(args.output())

    if (args.localtest()) {
      // Decompress the graph to verify correctness.

      val decompressedGraph = decompress(sc.objectFile(args.output()))
      val recoverPath = new Path( args.output() + "-recover" )
      FileSystem.get(sc.hadoopConfiguration).delete(recoverPath, true)

      decompressedGraph.edges.sortBy( e => (e.srcId, e.dstId), true, 1 ).map( e => e.srcId + " " + e.dstId )
        .saveAsTextFile(args.output() + "-recover")

      if ( graphEqual(graph, decompressedGraph ) ) {
        println("Compression/decompression test passed.")
      } else {
        println("Compression/decompression test failed.")
      }
      println("Please check the decompressed graph output at " + args.output() + "-recover" )
    }

    sc.stop()
  }
}
// scalastyle:on println
