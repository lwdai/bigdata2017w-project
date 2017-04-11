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

import _root_.io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.graphx._


class KBetweennessConfig(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val seeds = opt[Int](descr = "number of seeds (K)", required = true)
  val diameter = opt[Int](descr = "max number of iterations for one BFS", required = true)
  verify()
}

object KBetweenness {
  val log = Logger.getLogger(getClass().getName())

  // VD = (depth, #SP, credit, tmp_credit); ED = credit
  def initBFS( graph: Graph[(Int, Long, Double, Double), Double], root: Long):
      Graph[(Int, Long, Double, Double), Double] = {

    graph.mapVertices[(Int, Long, Double, Double)]((vid, v) =>
      ( if (vid == root) 0 else Integer.MAX_VALUE,
        if (vid == root) 1 else 0,
        0.0,
        0.0) )
  }

  // message = (depth of parent, number of shortest paths to root)
  def bfsVprog(vid: Long, vd: (Int, Long, Double, Double), msg: (Int, Long)): (Int, Long, Double, Double) = {
    ( if (vd._1 == Integer.MAX_VALUE && msg._1 < Integer.MAX_VALUE) msg._1 + 1 else vd._1,
      vd._2 + msg._2,
      0.0,
      0.0 )
  }

  def bfsSendMsg(e: EdgeTriplet[(Int, Long, Double, Double), Double]): Iterator[(VertexId, (Int, Long))] = {
    if ( e.srcAttr._1 < Integer.MAX_VALUE && e.dstAttr._1 == Integer.MAX_VALUE ) {
      // only send message if source vertex is visited and dst vertex is unvisited
      Iterator( (e.dstId, (e.srcAttr._1, e.srcAttr._2) ) )
    } else {
      Iterator.empty
    }
  }

  def bfsMergeMsg(msg1: (Int, Long), msg2: (Int, Long)): (Int, Long) = {
    ( // depth of parent
      if (msg1._1 < msg2._1) msg1._1 else msg2._1,
      // # SP
      if (msg1._1 < msg2._1) msg1._2
      else if (msg1._1 > msg2._1) msg2._2
      else msg1._2 + msg2._2)
  }

  // Add credit to the vertex
  def bottomUpVprog(vid: Long, vd: (Int, Long, Double, Double), msg: Double): (Int, Long, Double, Double) = {
    ( vd._1, vd._2, vd._3 + vd._4, msg )
  }

  def bottomUpSendMsg(e: EdgeTriplet[(Int, Long, Double, Double), Double]): Iterator[(VertexId, Double)] = {
    if ( e.srcAttr._1 == e.dstAttr._1 - 1 && e.dstAttr._4 > 0.0 ) {
      // src is parent of dst
      Iterator( (e.srcId, e.dstAttr._4 * e.srcAttr._2.toDouble / e.dstAttr._2.toDouble) )
    } else {
      Iterator.empty
    }
  }

  def bottomUpMergeMsg( msg1: Double, msg2: Double ): Double = {
    msg1 + msg2
  }

  def singleSrcBetweenness(graph: Graph[(Int, Long, Double, Double), Double], root: Long, maxIter: Int,
                            outputPath: String): Graph[(Int, Long, Double, Double), Double] = {
    val g = initBFS(graph, root).
      // BFS
      pregel[(Int, Long)]( (Integer.MAX_VALUE, 0), maxIter, EdgeDirection.Out)(
        bfsVprog, bfsSendMsg, bfsMergeMsg).
      // pop up credit
      pregel[Double]( 1.0, maxIter, EdgeDirection.In)(bottomUpVprog, bottomUpSendMsg, bottomUpMergeMsg)
      .mapVertices( (vid, vd) => (vd._1, vd._2, vd._3 + vd._4, 0.0) ).cache()

    if (outputPath != null) {
      g.vertices.map( v => v._1 + " " + v._2._1 + " " + v._2._2 + " " + v._2._3 ).saveAsTextFile(outputPath)
    }
    g.mapTriplets( et => {
      val edgeCredit: Double =
        if (et.srcAttr._1 == et.dstAttr._1 - 1) et.dstAttr._3 * et.srcAttr._2.toDouble / et.dstAttr._2.toDouble
        else 0.0

      edgeCredit + et.attr
    } )
  }

  def main(argv: Array[String]): Unit = {

    val args = new KBetweennessConfig(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("K-betweenness")
    val sc = new SparkContext(conf)

    //var graph: Graph[(Int, Long, Double), (Double, Double)] = CompressGraph.loadCompressedGraph(args.input(),
    //  sc)
    var graph: Graph[(Int, Long, Double, Double), Double] = GraphLoader.edgeListFile(sc, args.input())
      .mapVertices[(Int, Long, Double, Double)]( (vid, v) => (0, 0, 0.0, 0.0) ).
      mapEdges( e => 0.0).cache()


    1 to args.seeds() foreach { _ => {
      val root: Long = graph.pickRandomVertex()
      // message = (depth of parent, number of shortest paths to root)
      println("Using root id = " + root)
      val path = args.output() + "-root-" + root
      val outputDir = new Path(path)
      val deleted = FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
      if (deleted) println("output directory is deleted.")

      graph = singleSrcBetweenness(graph, root, args.diameter(), path)
    } }

    val path = args.output()
    val outputDir = new Path(path)
    val deleted = FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    graph.triplets.map( et => et.srcId + " " + et.dstId + " " + et.attr ).saveAsTextFile(path)

    sc.stop()
  }
}
// scalastyle:on println
