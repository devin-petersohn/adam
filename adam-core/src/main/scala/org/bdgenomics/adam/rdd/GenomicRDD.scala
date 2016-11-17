/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd

import java.util.concurrent.Executors
import org.apache.avro.generic.IndexedRecord
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkFiles
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  SequenceDictionary
}
import org.bdgenomics.formats.avro.{ Contig, RecordGroupMetadata, Sample }
import org.bdgenomics.utils.cli.SaveArgs
import scala.collection.mutable.{ ListBuffer, ArrayBuffer }
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import org.bdgenomics.adam.rdd.SortedGenomicRDD._
import util.control.Breaks._

private[rdd] class JavaSaveArgs(var outputPath: String,
                                var blockSize: Int = 128 * 1024 * 1024,
                                var pageSize: Int = 1 * 1024 * 1024,
                                var compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                                var disableDictionaryEncoding: Boolean = false,
                                var asSingleFile: Boolean = false) extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
  var deferMerging = false
}

private[rdd] object GenomicRDD {

  /**
   * Replaces file references in a command.
   *
   * @see pipe
   * @param cmd Command to split and replace references in.
   * @param files List of paths to files.
   * @return Returns a split up command string, with file paths subbed in.
   */
  def processCommand(cmd: String,
                     files: Seq[String]): List[String] = {
    val filesWithIndex = files.zipWithIndex
      .map(p => {
        val (file, index) = p
        ("$%d".format(index), file)
      }).reverse

    @tailrec def replaceEscapes(cmd: String,
                                iter: Iterator[(String, String)]): String = {
      if (!iter.hasNext) {
        cmd
      } else {
        val (idx, file) = iter.next
        val newCmd = cmd.replace(idx, file)
        replaceEscapes(newCmd, iter)
      }
    }

    cmd.split(" ")
      .map(s => {
        replaceEscapes(s, filesWithIndex.toIterator)
      }).toList
  }
}

trait GenomicRDD[T, U <: GenomicRDD[T, U]] {

  val rdd: RDD[T]

  val sequences: SequenceDictionary

  lazy val jrdd: JavaRDD[T] = {
    rdd.toJavaRDD()
  }

  def transform(tFn: RDD[T] => RDD[T]): U = {
    replaceRdd(tFn(rdd))
  }

  lazy val starts: RDD[Long] = flattenRddByRegions().map(f => f._1.start)

  lazy val elements: Long = starts.max

  lazy val minimum: Long = starts.min

  /**
   * Pipes genomic data to a subprocess that runs in parallel using Spark.
   *
   * Files are substituted in to the command with a $x syntax. E.g., to invoke
   * a command that uses the first file from the files Seq, use $0.
   *
   * Pipes require the presence of an InFormatterCompanion and an OutFormatter
   * as implicit values. The InFormatterCompanion should be a singleton whose
   * apply method builds an InFormatter given a specific type of GenomicRDD.
   * The implicit InFormatterCompanion yields an InFormatter which is used to
   * format the input to the pipe, and the implicit OutFormatter is used to
   * parse the output from the pipe.
   *
   * @param cmd Command to run.
   * @param files Files to make locally available to the commands being run.
   *   Default is empty.
   * @param environment A map containing environment variable/value pairs to set
   *   in the environment for the newly created process. Default is empty.
   * @param flankSize Number of bases to flank each command invocation by.
   * @return Returns a new GenomicRDD of type Y.
   * @tparam X The type of the record created by the piped command.
   * @tparam Y A GenomicRDD containing X's.
   * @tparam V The InFormatter to use for formatting the data being piped to the
   *   command.
   */
  def pipe[X, Y <: GenomicRDD[X, Y], V <: InFormatter[T, U, V]](cmd: String,
                                                                files: Seq[String] = Seq.empty,
                                                                environment: Map[String, String] = Map.empty,
                                                                flankSize: Int = 0)(implicit tFormatterCompanion: InFormatterCompanion[T, U, V],
                                                                                    xFormatter: OutFormatter[X],
                                                                                    convFn: (U, RDD[X]) => Y,
                                                                                    tManifest: ClassTag[T],
                                                                                    xManifest: ClassTag[X]): Y = {

    // TODO: support broadcasting files
    files.foreach(f => {
      rdd.context.addFile(f)
    })

    // make formatter
    val tFormatter: V = tFormatterCompanion.apply(this.asInstanceOf[U])

    // make bins
    val seqLengths = sequences.records.toSeq.map(rec => (rec.name, rec.length)).toMap
    val totalLength = seqLengths.values.sum
    val bins = GenomeBins(totalLength / rdd.partitions.size, seqLengths)

    // if the input rdd is mapped, then we need to repartition
    val partitionedRdd = if (sequences.records.size > 0) {
      // get region covered, expand region by flank size, and tag with bins
      val binKeyedRdd = rdd.flatMap(r => {

        // get regions and expand
        val regions = getReferenceRegions(r).map(_.pad(flankSize))

        // get all the bins this record falls into
        val recordBins = regions.flatMap(rr => {
          (bins.getStartBin(rr) to bins.getEndBin(rr)).map(b => (rr, b))
        })

        // key the record by those bins and return
        // TODO: this should key with the reference region corresponding to a bin
        recordBins.map(b => (b, r))
      })

      // repartition yonder our data
      // TODO: this should repartition and sort within the partition
      binKeyedRdd.repartitionAndSortWithinPartitions(ManualRegionPartitioner(bins.numBins))
        .values
    } else {
      rdd
    }

    // are we in local mode?
    val isLocal = partitionedRdd.context.isLocal

    // call map partitions and pipe
    val pipedRdd = partitionedRdd.mapPartitions(iter => {

      // get files
      // from SPARK-3311, SparkFiles doesn't work in local mode.
      // so... we'll bypass that by checking if we're running in local mode.
      // sigh!
      val locs = if (isLocal) {
        files
      } else {
        files.map(f => {
          SparkFiles.get(f)
        })
      }

      // split command and create process builder
      val finalCmd = GenomicRDD.processCommand(cmd, locs)
      val pb = new ProcessBuilder(finalCmd)
      pb.redirectError(ProcessBuilder.Redirect.INHERIT)

      // add environment variables to the process builder
      val pEnv = pb.environment()
      environment.foreach(kv => {
        val (k, v) = kv
        pEnv.put(k, v)
      })

      // start underlying piped command
      val process = pb.start()
      val os = process.getOutputStream()
      val is = process.getInputStream()

      // wrap in and out formatters
      val ifr = new InFormatterRunner[T, U, V](iter, tFormatter, os)
      val ofr = new OutFormatterRunner[X, OutFormatter[X]](xFormatter, is)

      // launch thread pool and submit formatters
      val pool = Executors.newFixedThreadPool(2)
      pool.submit(ifr)
      val futureIter = pool.submit(ofr)

      // wait for process to finish
      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw new RuntimeException("Piped command %s exited with error code %d.".format(
          finalCmd, exitCode))
      }

      // shut thread pool
      pool.shutdown()

      futureIter.get
    })

    // build the new GenomicRDD
    val newRdd = convFn(this.asInstanceOf[U], pipedRdd)

    // if the original rdd was aligned and the final rdd is aligned, then we must filter
    if (newRdd.sequences.isEmpty ||
      sequences.isEmpty) {
      newRdd
    } else {
      def filterPartition(idx: Int, iter: Iterator[X]): Iterator[X] = {

        // get the region for this partition
        val region = bins.invert(idx)

        // map over the iterator and filter out any items that don't belong
        iter.filter(x => {

          // get the regions for x
          val regions = newRdd.getReferenceRegions(x)

          // are there any regions that overlap our current region
          !regions.forall(!_.overlaps(region))
        })
      }

      // run a map partitions with index and discard all items that fall outside of their
      // own partition's region bound
      newRdd.transform(_.mapPartitionsWithIndex(filterPartition))
    }
  }

  protected[rdd] def replaceRdd(newRdd: RDD[T]): U

  protected[rdd] def getReferenceRegions(elem: T): Seq[ReferenceRegion]

  protected[rdd] def flattenRddByRegions(): RDD[(ReferenceRegion, T)] = {
    rdd.flatMap(elem => {
      getReferenceRegions(elem).map(r => (r, elem))
    })
  }

  def filterByOverlappingRegion(query: ReferenceRegion): U = {
    replaceRdd(rdd.filter(elem => {

      // where can this item sit?
      val regions = getReferenceRegions(elem)

      // do any of these overlap with our query region?
      regions.exists(_.overlaps(query))
    }))
  }

  /**
   * Performs a broadcast inner join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is an inner join, all values who do not overlap a value from the other
   * RDD are dropped.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def broadcastRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](genomicRdd: GenomicRDD[X, Y])(
    implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, X), Z] = {

    // key the RDDs and join
    GenericGenomicRDD[(T, X)](InnerBroadcastRegionJoin[T, X]().partitionAndJoin(flattenRddByRegions(),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => { getReferenceRegions(kv._1) ++ genomicRdd.getReferenceRegions(kv._2) })
      .asInstanceOf[GenomicRDD[(T, X), Z]]
  }

  /**
   * Performs a broadcast right outer join between this RDD and another RDD.
   *
   * In a broadcast join, the left RDD (this RDD) is collected to the driver,
   * and broadcast to all the nodes in the cluster. The key equality function
   * used for this join is the reference region overlap function. Since this
   * is a right outer join, all values in the left RDD that do not overlap a
   * value from the right RDD are dropped. If a value from the right RDD does
   * not overlap any values in the left RDD, it will be paired with a `None`
   * in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterBroadcastRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], X), Z]](genomicRdd: GenomicRDD[X, Y])(
    implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], X), Z] = {

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], X)](RightOuterBroadcastRegionJoin[T, X]().partitionAndJoin(flattenRddByRegions(),
      genomicRdd.flattenRddByRegions()),
      sequences ++ genomicRdd.sequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
      .asInstanceOf[GenomicRDD[(Option[T], X), Z]]
  }

  /**
   * Performs a sort-merge inner join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is an inner join, all values who do not
   * overlap a value from the other RDD are dropped.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space.
   */
  def shuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, X), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                              optPartitions: Option[Int] = None)(
                                                                                implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, X), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd = repartitionAndSortByGenomicCoordinate(partitions).evenlyRepartition(partitions)
    val rightRdd = {
      genomicRdd match {
        case in: SortedGenomicRDDMixIn[X, Y] =>
          println("That is sorted but this is not")
          in.coPartitionByGenomicRegion(leftRdd.asInstanceOf[SortedGenomicRDDMixIn[X, Y]])
        case _ =>
          println("Both not sorted")
          genomicRdd.repartitionAndSortByGenomicCoordinate(partitions).coPartitionByGenomicRegion(leftRdd.asInstanceOf[SortedGenomicRDDMixIn[X, Y]])
      }
    }

    val leftRddReadyToJoin = leftRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => ((leftRdd.indexedReferenceRegions(f._2.toInt), idx), f._1))
    })
    val rightRddReadytoJoin = rightRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => ((rightRdd.indexedReferenceRegions(f._2.toInt), idx), f._1))
    })
    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(T, X)](
      InnerShuffleRegionJoin[T, X](endSequences, partitions, rdd.context).joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadytoJoin),
      endSequences,
      kv => { getReferenceRegions(kv._1) ++ rightRdd.getReferenceRegions(kv._2) })
      .asInstanceOf[GenomicRDD[(T, X), Z]]
  }

  /**
   * Performs a sort-merge right outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a right outer join, all values in the
   * left RDD that do not overlap a value from the right RDD are dropped.
   * If a value from the right RDD does not overlap any values in the left
   * RDD, it will be paired with a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   right RDD that did not overlap a key in the left RDD.
   */
  def rightOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], X), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                optPartitions: Option[Int] = None)(
                                                                                                  implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], X), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], X)](
      RightOuterShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => {
        Seq(kv._1.map(v => getReferenceRegions(v))).flatten.flatten ++
          genomicRdd.getReferenceRegions(kv._2)
      })
      .asInstanceOf[GenomicRDD[(Option[T], X), Z]]
  }

  /**
   * Performs a sort-merge left outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a left outer join, all values in the
   * right RDD that do not overlap a value from the left RDD are dropped.
   * If a value from the left RDD does not overlap any values in the right
   * RDD, it will be paired with a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and all keys from the
   *   left RDD that did not overlap a key in the right RDD.
   */
  def leftOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Option[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                               optPartitions: Option[Int] = None)(
                                                                                                 implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, Option[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(T, Option[X])](
      LeftOuterShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => {
        Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v))).flatten.flatten ++
          getReferenceRegions(kv._1)
      })
      .asInstanceOf[GenomicRDD[(T, Option[X]), Z]]
  }

  /**
   * Performs a sort-merge full outer join between this RDD and another RDD.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is a full outer join, if a value from either
   * RDD does not overlap any values in the other RDD, it will be paired with
   * a `None` in the product of the join.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, and values that did not
   *   overlap will be paired with a `None`.
   */
  def fullOuterShuffleRegionJoin[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Option[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                       optPartitions: Option[Int] = None)(
                                                                                                         implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], Option[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], Option[X])](
      FullOuterShuffleRegionJoin[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => {
        Seq(kv._2.map(v => genomicRdd.getReferenceRegions(v)),
          kv._1.map(v => getReferenceRegions(v))).flatten.flatten
      })
      .asInstanceOf[GenomicRDD[(Option[T], Option[X]), Z]]
  }

  /**
   * Performs a sort-merge inner join between this RDD and another RDD,
   * followed by a groupBy on the left value.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. Since this is an inner join, all values who do not
   * overlap a value from the other RDD are dropped. In the same operation,
   * we group all values by the left item in the RDD.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD..
   */
  def shuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(T, Iterable[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                      optPartitions: Option[Int] = None)(
                                                                                                        implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(T, Iterable[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    val leftRdd = repartitionAndSortByGenomicCoordinate(partitions).evenlyRepartition(partitions)
    val rightRdd = {
      genomicRdd match {
        case in: SortedGenomicRDDMixIn[X, Y] =>
          println("That is sorted but this is not")
          in.coPartitionByGenomicRegion(leftRdd.asInstanceOf[SortedGenomicRDDMixIn[X, Y]])
        case _ =>
          println("Both not sorted")
          genomicRdd.repartitionAndSortByGenomicCoordinate(partitions).coPartitionByGenomicRegion(leftRdd.asInstanceOf[SortedGenomicRDDMixIn[X, Y]])
      }
    }

    val leftRddReadyToJoin = leftRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => ((leftRdd.indexedReferenceRegions(f._2.toInt), idx), f._1))
    })
    val rightRddReadytoJoin = rightRdd.rdd.zipWithIndex.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => ((rightRdd.indexedReferenceRegions(f._2.toInt), idx), f._1))
    })
    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences
    println("Preparing for partitionMap")
    val joinedRDD = InnerShuffleRegionJoin[T, X](endSequences,
      partitions,
      rdd.context).joinCoPartitionedRdds(leftRddReadyToJoin, rightRddReadytoJoin).mapPartitionsWithIndex((idx, iter) => {
        if (iter.isEmpty) Iterator()
        else {
          val listRepresentation = iter.toList
          listRepresentation.groupBy(_._1).mapValues(_.map(_._2).toIterable).toIterator
        }
      })
    // key the RDDs and join
    GenericGenomicRDD[(T, Iterable[X])](
      joinedRDD,
      endSequences,
      kv => {
        (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
          getReferenceRegions(kv._1)).toSeq
      })
      .asInstanceOf[GenomicRDD[(T, Iterable[X]), Z]]
  }

  /**
   * Performs a sort-merge right outer join between this RDD and another RDD,
   * followed by a groupBy on the left value, if not null.
   *
   * In a sort-merge join, both RDDs are co-partitioned and sorted. The
   * partitions are then zipped, and we do a merge join on each partition.
   * The key equality function used for this join is the reference region
   * overlap function. In the same operation, we group all values by the left
   * item in the RDD. Since this is a right outer join, all values from the
   * right RDD who did not overlap a value from the left RDD are placed into
   * a length-1 Iterable with a `None` key.
   *
   * @param genomicRdd The right RDD in the join.
   * @return Returns a new genomic RDD containing all pairs of keys that
   *   overlapped in the genomic coordinate space, grouped together by
   *   the value they overlapped in the left RDD, and all values from the
   *   right RDD that did not overlap an item in the left RDD.
   */
  def rightOuterShuffleRegionJoinAndGroupByLeft[X, Y <: GenomicRDD[X, Y], Z <: GenomicRDD[(Option[T], Iterable[X]), Z]](genomicRdd: GenomicRDD[X, Y],
                                                                                                                        optPartitions: Option[Int] = None)(
                                                                                                                          implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenomicRDD[(Option[T], Iterable[X]), Z] = {

    // did the user provide a set partition count?
    // if no, take the max partition count from our rdds
    val partitions = optPartitions.getOrElse(Seq(rdd.partitions.length,
      genomicRdd.rdd.partitions.length).max)

    // what sequences do we wind up with at the end?
    val endSequences = sequences ++ genomicRdd.sequences

    // key the RDDs and join
    GenericGenomicRDD[(Option[T], Iterable[X])](
      RightOuterShuffleRegionJoinAndGroupByLeft[T, X](endSequences,
        partitions,
        rdd.context).partitionAndJoin(flattenRddByRegions(),
          genomicRdd.flattenRddByRegions()),
      endSequences,
      kv => {
        (kv._2.flatMap(v => genomicRdd.getReferenceRegions(v)) ++
          kv._1.toSeq.flatMap(v => getReferenceRegions(v))).toSeq
      })
      .asInstanceOf[GenomicRDD[(Option[T], Iterable[X]), Z]]
  }

  /**
    * This method is the first method called when there is some need for the
    * data to be sorted. We are assuming no previous knowledge about data
    * distribution or skew, and as such we simply partition based on range.
    * This can present some issues when the data is extremely skewed, but
    * we attempt to do our best in solving this problem.
    *
    * The output from this method is a SortedGenomicRDD, which simply
    * contains the knowledge that this was sorted and overrides many methods
    * that would be improved by sorting the data first.
    *
    * @param partitions The number of partitions to split the data into
    * @return A SortedGenomicRDDMixIn that contains the sorted and partitioned
    *         RDD
   */
  def repartitionAndSortByGenomicCoordinate(partitions: Int = rdd.partitions.length)(implicit c: ClassTag[T]): SortedGenomicRDDMixIn[T, U] = {
    starts.persist
    elements
    minimum

    //var seqLengths = Map(sequences.records.toSeq.map(rec => (rec.name, rec.length)): _*)
    //val bins = rdd.context.broadcast(GenomeBins(partitions, seqLengths))
    val regionedRDD = flattenRddByRegions()
    val sample = regionedRDD.sample(false, 0.01, 1337)
    // we want to know exactly how much data is on each node
    val partitionTupleCounts: Array[Int] = sample.mapPartitions(f => Iterator(f.size)).collect
    // the average number of records on each node will help us evenly repartition
    val average: Double = partitionTupleCounts.sum.toDouble / partitions
    val partitionMap = sample.keyBy(f => (f._1.referenceName, f._1.start, f._1.end)).sortByKey(true, partitions)
      .zipWithIndex
      .mapPartitions(iter => {
        iter.map(f => ((f._2/average).toInt, f._1._2))
      })
      .partitionBy(new GenomicPositionRangePartitioner(partitions))
      .mapPartitions(iter => {
        if(iter.isEmpty) Iterator()
        else {
          val listRepresentation = iter.toList
          Iterator((listRepresentation.head._2._1, listRepresentation.last._2._1))
        }
      }).collect

    val partitionedRDD = if(false) {
      regionedRDD.keyBy(f => (f._1.referenceName, f._1.start, f._1.end))
        .mapPartitions(iter => {
          val listRepresentation = iter.toList
          listRepresentation.sortBy(f => (f._1._1, f._1._2, f._1._3)).toIterator
        }).values
        .mapPartitions(iter => getDestinationPartition(partitionMap,iter), preservesPartitioning = true)
        .partitionBy(new GenomicPositionRangePartitioner(partitions))
        .mapPartitions(iter => {
          val listRepresentation = iter.toList.map(_._2)
          val finalTempList = new ListBuffer[((ReferenceRegion, T))]
          while(listRepresentation.exists(_.nonEmpty)) {
            val tempList = new ListBuffer[((ReferenceRegion, T), Int)]
            for (i <- listRepresentation.indices) {
              breakable {
                try {
                  tempList += ((listRepresentation(i).head, i))
                } catch {
                  case e: NoSuchElementException => break
                }
              }
            }
          }
          finalTempList.toIterator
        }, preservesPartitioning = true)
        .zipWithIndex
    } else regionedRDD.sortBy(f => (f._1.referenceName, f._1.start, f._1.end), true, partitions).zipWithIndex

    //.map(f => (bins.value.getStartBin(f._1), (f)))
    //.partitionBy(new GenomicPositionRangePartitioner(partitions, elements.toInt)) //.values
    //.mapPartitions(iter => iter.toArray.sortBy(tuple => (tuple._1.referenceName, tuple._1.start, tuple._1.end)).toIterator).zipWithIndex()

    addSortedTrait(replaceRdd(partitionedRDD.keys.values), partitionedRDD.keys.keys.collect,
      partitionedRDD.values.mapPartitions(iter => {
        if (iter.isEmpty) Iterator()
        else {
          val listRepresentation = iter.toList
          Iterator((listRepresentation.head, listRepresentation.last))
        }
      }).collect)
  }

  def getDestinationPartition(partitionMap: Seq[(ReferenceRegion, ReferenceRegion)], iter: Iterator[(ReferenceRegion, T)]): Iterator[(Int, ListBuffer[(ReferenceRegion, T)])] = {
    val listRepresentation = iter.toList
    val outputList = new ListBuffer[(Int, ListBuffer[(ReferenceRegion, T)])]
    for(i <- partitionMap.indices) {
      if(i != 0 && i != partitionMap.length - 1) {
        outputList += ((i, listRepresentation.filter(f =>
          f._1.referenceName >= partitionMap(i)._1.referenceName &&
            f._1.start >= partitionMap(i)._1.start &&
            f._1.end >= partitionMap(i)._1.end &&
            f._1.referenceName < partitionMap(i)._2.referenceName &&
            f._1.start < partitionMap(i)._2.start &&
            f._1.end < partitionMap(i)._2.end
        ).asInstanceOf[ListBuffer[(ReferenceRegion, T)]]))
      } else if(i == 0) {
        outputList += ((i, listRepresentation.filter(f =>
          f._1.referenceName < partitionMap(i)._2.referenceName &&
          f._1.start < partitionMap(i)._2.start &&
          f._1.end < partitionMap(i)._2.end
          ).asInstanceOf[ListBuffer[(ReferenceRegion, T)]]))
      } else {
        outputList += ((i, listRepresentation.filter(f =>
        f._1.referenceName >= partitionMap(i)._1.referenceName &&
          f._1.start >= partitionMap(i)._1.start &&
          f._1.end >= partitionMap(i)._1.end
        ).asInstanceOf[ListBuffer[(ReferenceRegion, T)]]))
      }
    }
    outputList.toIterator
  }

  def wellBalancedRepartitionByGenomicCoordinate(partitions: Int = rdd.partitions.length)(implicit tTag: ClassTag[T]): SortedGenomicRDDMixIn[T, U] = {
    repartitionAndSortByGenomicCoordinate(partitions).evenlyRepartition(partitions)
  }

  private[rdd] class GenomicPositionRangePartitioner[V](partitions: Int, elements: Int = 0) extends Partitioner {

    override def numPartitions: Int = partitions

    def getRegionPartition(key: ReferenceRegion): Int = {
      val partitionNumber = if ((key.start.toInt - minimum.toInt) * partitions / (elements - minimum.toInt) == partitions)
        Math.abs((key.start.toInt - minimum.toInt) * partitions / (elements - minimum.toInt) - 1)
      else Math.abs((key.start.toInt - minimum.toInt) * partitions / (elements - minimum.toInt))
      partitionNumber
    }

    def getPartition(key: Any): Int = {
      key match {
        case f: ReferenceRegion     => getRegionPartition(f)
        case (f1: Int, f2: Boolean) => f1
        case f: Int                 => f
        case _                      => throw new Exception("Reference Region Key require to partition on Genomic Position")
      }
    }

  }
}

private case class GenericGenomicRDD[T](rdd: RDD[T],
                                        sequences: SequenceDictionary,
                                        regionFn: T => Seq[ReferenceRegion]) extends GenomicRDD[T, GenericGenomicRDD[T]] {

  protected[rdd] def replaceRdd(newRdd: RDD[T]): GenericGenomicRDD[T] = {
    copy(rdd = newRdd)
  }

  protected[rdd] def getReferenceRegions(elem: T): Seq[ReferenceRegion] = {
    regionFn(elem)
  }
}

trait MultisampleGenomicRDD[T, U <: MultisampleGenomicRDD[T, U]] extends GenomicRDD[T, U] {

  val samples: Seq[Sample]
}

abstract class AvroReadGroupGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroReadGroupGenomicRDD[T, U]] extends AvroGenomicRDD[T, U] {

  val recordGroups: RecordGroupDictionary

  override protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    val contigSchema = Contig.SCHEMA$
    //if (sorted) contigSchema.addProp("sorted", { "sorted" -> "true" })
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      contigSchema,
      contigs)

    // convert record group to avro and save
    val rgMetadata = recordGroups.recordGroups
      .map(_.toMetadata)
    val recordGroupMetaData = RecordGroupMetadata.SCHEMA$
    //if (sorted) recordGroupMetaData.addProp("sorted", { "sorted" -> "true })
    saveAvro("%s/_rgdict.avro".format(filePath),
      rdd.context,
      recordGroupMetaData,
      rgMetadata)
  }
}

abstract class MultisampleAvroGenomicRDD[T <% IndexedRecord: Manifest, U <: MultisampleAvroGenomicRDD[T, U]] extends AvroGenomicRDD[T, U]
    with MultisampleGenomicRDD[T, U] {

  override protected def saveMetadata(filePath: String) {

    val sampleSchema = Sample.SCHEMA$
    //if (sorted) sampleSchema.addProp("sorted", { "sorted" -> "true" })
    // get file to write to
    saveAvro("%s/_samples.avro".format(filePath),
      rdd.context,
      sampleSchema,
      samples)

    val contigSchema = Contig.SCHEMA$
    //if (sorted) contigSchema.addProp("sorted", { "sorted" -> "true" })
    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      contigSchema,
      contigs)
  }
}

abstract class AvroGenomicRDD[T <% IndexedRecord: Manifest, U <: AvroGenomicRDD[T, U]] extends ADAMRDDFunctions[T]
    with GenomicRDD[T, U] { // with SortedGenomicRDD[T, U] {

  /**
   * Called in saveAsParquet after saving RDD to Parquet to save metadata.
   *
   * Writes any necessary metadata to disk. If not overridden, writes the
   * sequence dictionary to disk as Avro.
   *
   * @param args Arguments for saving file to disk.
   */
  protected def saveMetadata(filePath: String) {

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    val contigSchema = Contig.SCHEMA$
    //if (sorted) contigSchema.addProp("sorted", { "sorted" -> "true" })
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      contigSchema,
      contigs)
  }

  /**
   * Saves RDD as a directory of Parquet files.
   *
   * The RDD is written as a directory of Parquet files, with
   * Parquet configuration described by the input param args.
   * The provided sequence dictionary is written at args.outputPath/_seqdict.avro
   * as Avro binary.
   *
   * @param args Save configuration arguments.
   */
  def saveAsParquet(args: SaveArgs) {
    saveAsParquet(
      args.outputPath,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionaryEncoding
    )
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   *   Default is false.
   */
  def saveAsParquet(
    filePath: String,
    blockSize: Int = 128 * 1024 * 1024,
    pageSize: Int = 1 * 1024 * 1024,
    compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
    disableDictionaryEncoding: Boolean = false) {
    saveRddAsParquet(filePath,
      blockSize,
      pageSize,
      compressCodec,
      disableDictionaryEncoding)
    saveMetadata(filePath)
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   * @param blockSize Size per block.
   * @param pageSize Size per page.
   * @param compressCodec Name of the compression codec to use.
   * @param disableDictionaryEncoding Whether or not to disable bit-packing.
   */
  def saveAsParquet(
    filePath: java.lang.String,
    blockSize: java.lang.Integer,
    pageSize: java.lang.Integer,
    compressCodec: CompressionCodecName,
    disableDictionaryEncoding: java.lang.Boolean) {
    saveAsParquet(
      new JavaSaveArgs(filePath,
        blockSize = blockSize,
        pageSize = pageSize,
        compressionCodec = compressCodec,
        disableDictionaryEncoding = disableDictionaryEncoding))
  }

  /**
   * Saves this RDD to disk as a Parquet file.
   *
   * @param filePath Path to save the file at.
   */
  def saveAsParquet(filePath: java.lang.String) {
    saveAsParquet(new JavaSaveArgs(filePath))
  }
}

trait Unaligned {

  val sequences = SequenceDictionary.empty
}
