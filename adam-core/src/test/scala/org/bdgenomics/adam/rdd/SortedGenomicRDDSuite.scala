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

import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary, ReferenceRegion}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.variant.GenotypeRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.misc.SparkFunSuite
import scala.collection.mutable.ListBuffer

class SortedGenomicRDDSuite extends SparkFunSuite {

  /**
   * Determines if a given partition map has been correctly sorted
   *
   * @param list The partition map
   * @return a boolean where true is sorted and false is unsorted
   */
  def isSorted(list: Seq[Option[(ReferenceRegion, ReferenceRegion)]]): Boolean = {
    val test = list.drop(1).map(_.get._1)
    val test2 = list.dropRight(1).map(_.get._2)
    !test2.zip(test).exists(f => f._1.start > f._2.start && f._1.end > f._2.end && f._1.referenceName > f._2.referenceName)
  }
  val chromosomeLengths = Map(1 -> 248956422, 2 -> 242193529, 3 -> 198295559, 4 -> 190214555, 5 -> 181538259, 6 -> 170805979, 7 -> 159345973, 8 -> 145138636, 9 -> 138394717, 10 -> 133797422, 11 -> 135086622, 12 -> 133275309, 13 -> 114364328, 14 -> 107043718, 15 -> 101991189, 16 -> 90338345, 17 -> 83257441, 18 -> 80373285, 19 -> 58617616, 20 -> 64444167, 21 -> 46709983, 22 -> 50818468)

  val sd = new SequenceDictionary(Vector(
    SequenceRecord("chr20",63025520,None,None,None,None,None,None,Some(19)),
    SequenceRecord("chr7",159138663,None,None,None,None,None,None,Some(6)),
    SequenceRecord("chr18",78077248,None,None,None,None,None,None,referenceIndex=Some(17)),
    SequenceRecord("chr13",115169878,None,None,None,None,None,None,referenceIndex=Some(12)),
    SequenceRecord("chr3",198022430,None,None,None,None,None,None,referenceIndex=Some(2)),
    SequenceRecord("chr6",171115067,None,None,None,None,None,None,referenceIndex=Some(5)),
    SequenceRecord("chr9",141213431,None,None,None,None,None,None,referenceIndex=Some(8)),
    SequenceRecord("chr16",90354753,None,None,None,None,None,None,referenceIndex=Some(15)),
    SequenceRecord("chr10",135534747,None,None,None,None,None,None,referenceIndex=Some(9)),
    SequenceRecord("chr12",133851895,None,None,None,None,None,None,referenceIndex=Some(11)),
    SequenceRecord("chr8",146364022,None,None,None,None,None,None,referenceIndex=Some(7)),
    SequenceRecord("chr19",59128983,None,None,None,None,None,None,referenceIndex=Some(18)),
    SequenceRecord("chr2",243199373,None,None,None,None,None,None,referenceIndex=Some(1)),
    SequenceRecord("chr15",102531392,None,None,None,None,None,None,referenceIndex=Some(14)),
    SequenceRecord("chr14",107349540,None,None,None,None,None,None,referenceIndex=Some(13)),
    SequenceRecord("chr17",81195210,None,None,None,None,None,None,referenceIndex=Some(16)),
    SequenceRecord("chr5",180915260,None,None,None,None,None,None,referenceIndex=Some(4)),
    SequenceRecord("chr4",191154276,None,None,None,None,None,None,referenceIndex=Some(3)),
    SequenceRecord("chr1",249250621,None,None,None,None,None,None,referenceIndex=Some(0)),
    SequenceRecord("chr21",48129895,None,None,None,None,None,None,referenceIndex=Some(20)),
    SequenceRecord("chr11,",135006516,None,None,None,None,None,None,referenceIndex=Some(10))
  ))

  sparkTest("testing that partition and sort provide correct outputs") {
    // load in a generic bam
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    // sort and make into 16 partitions
    val y = x.repartitionAndSort(16)
    assert(isSorted(y.partitionMap.get))
    // sort and make into 32 partitions
    val z = x.repartitionAndSort(32)
    assert(isSorted(z.partitionMap.get))
    val arrayRepresentationOfZ = z.rdd.collect
    //verify sort worked on actual values
    for (currentArray <- List(y.rdd.collect, z.rdd.collect)) {
      for (i <- currentArray.indices) {
        if (i != 0) assert(arrayRepresentationOfZ(i).getStart > arrayRepresentationOfZ(i - 1).getStart ||
          (arrayRepresentationOfZ(i).getStart == arrayRepresentationOfZ(i - 1).getStart && arrayRepresentationOfZ(i).getEnd >= arrayRepresentationOfZ(i - 1).getEnd))
      }
    }

    val partitionTupleCounts: Array[Int] = z.rdd.mapPartitions(f => Iterator(f.size)).collect
    val partitionTupleCounts2: Array[Int] = y.rdd.mapPartitions(f => Iterator(f.size)).collect
    // make sure that we didn't lose any data
    assert(partitionTupleCounts.sum == partitionTupleCounts2.sum)
  }

  sparkTest("testing copartition maintains or adds sort") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(16)
    val y = x.repartitionAndSort(32)
    val a = x.copartitionByReferenceRegion(y)
    val b = z.copartitionByReferenceRegion(y)
    assert(isSorted(a.partitionMap.get))
    assert(isSorted(b.partitionMap.get))

    val starts = z.rdd.map(f => f.getStart)
  }

  sparkTest("testing that we don't drop any data on the right side even though it doesn't map to a partition on the left") {
    // testing the left side with an extremely large region that is
    // not the last record on a partition
    // this test also tests the case that our
    val genotypeRddBuilder = new ListBuffer[Genotype]()

    genotypeRddBuilder += {
      Genotype.newBuilder()
        .setContigName("chr1")
        .setStart(2L)
        .setEnd(100L)
        .setVariant(
          Variant.newBuilder()
            .setStart(2L)
            .setEnd(100L)
            .setAlternateAllele("A")
            .setReferenceAllele("T")
            .build()
        )
        .setSampleId("1")
        .build()
    }

    genotypeRddBuilder += {
      Genotype.newBuilder()
        .setContigName("chr1")
        .setStart(3L)
        .setEnd(5L)
        .setVariant(
          Variant.newBuilder()
            .setStart(3L)
            .setEnd(5L)
            .setAlternateAllele("A")
            .setReferenceAllele("T")
            .build()
        )
        .setSampleId("2")
        .build()
    }

    genotypeRddBuilder += {
      Genotype.newBuilder()
        .setContigName("chr1")
        .setStart(6L)
        .setEnd(7L)
        .setVariant(
          Variant.newBuilder()
            .setStart(6L)
            .setEnd(7L)
            .setAlternateAllele("A")
            .setReferenceAllele("T")
            .build()
        )
        .setSampleId("3")
        .build()
    }

    genotypeRddBuilder += {
      Genotype.newBuilder()
        .setContigName("chr1")
        .setStart(8L)
        .setEnd(12L)
        .setVariant(
          Variant.newBuilder()
            .setStart(8L)
            .setEnd(12L)
            .setAlternateAllele("A")
            .setReferenceAllele("T")
            .build()
        )
        .setSampleId("3")
        .build()
    }

    val featureRddBuilder = new ListBuffer[Feature]()

    featureRddBuilder += {
      Feature.newBuilder()
        .setContigName("chr1")
        .setStart(61L)
        .setEnd(62L)
        .build()
    }

    featureRddBuilder += {
      Feature.newBuilder()
        .setContigName("chr1")
        .setStart(11L)
        .setEnd(15L)
        .build()
    }

    featureRddBuilder += {
      Feature.newBuilder()
        .setContigName("chr1")
        .setStart(3L)
        .setEnd(6L)
        .build()
    }

    featureRddBuilder += {
      Feature.newBuilder()
        .setContigName("chr1")
        .setStart(6L)
        .setEnd(8L)
        .build()
    }

    featureRddBuilder += {
      Feature.newBuilder()
        .setContigName("chr1")
        .setStart(50L)
        .setEnd(52L)
        .build()
    }

    featureRddBuilder += {
      Feature.newBuilder()
        .setContigName("chr1")
        .setStart(1L)
        .setEnd(2L)
        .build()
    }

    val genotypes = GenotypeRDD(sc.parallelize(genotypeRddBuilder), sd, Seq()).repartitionAndSort(2)
    genotypes.rdd.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => (idx, f))
    }).collect.foreach(println)
    val features = FeatureRDD(sc.parallelize(featureRddBuilder), sd)
    val x = features.copartitionByReferenceRegion(genotypes)
    val z = x.rdd.mapPartitionsWithIndex((idx, iter) => {
      if(idx == 0 && iter.size != 6) {
        Iterator(true)
      } else if(idx == 1 && iter.size != 2) {
        Iterator(true)
      } else {
        Iterator()
      }
    })

    x.rdd.mapPartitionsWithIndex((idx, iter) => {
      iter.map(f => (idx, f))
    }).collect.foreach(println)
    assert(z.collect.length == 0)


  }

  sparkTest("testing that sorted shuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    // sort and make into 16 partitions
    val z = x.repartitionAndSort(16)
    // perform join using 32 partitions
    val b = z.shuffleRegionJoin(x, Some(1))
    val c = x.shuffleRegionJoin(z, Some(1))
    val d = c.rdd.map(f => (f._1.getStart, f._2.getEnd)).collect.toSet
    val e = b.rdd.map(f => (f._1.getStart, f._2.getEnd)).collect.toSet

    val setDiff = d -- e
    assert(setDiff.isEmpty)

    println(b.partitionMap.get.mkString(","))
    println(z.partitionMap.get.mkString(","))
    println(b.rdd.count)
    assert(b.rdd.count == c.rdd.count)
  }

  sparkTest("testing that sorted fullOuterShuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(16)
    val d = x.fullOuterShuffleRegionJoin(z, Some(1))
    val e = z.fullOuterShuffleRegionJoin(x, Some(1))

    val setDiff = d.rdd.collect.toSet -- e.rdd.collect.toSet
    assert(setDiff.isEmpty)
    println(d.rdd.count)
    assert(d.rdd.count == e.rdd.count)
  }

  sparkTest("testing that sorted rightOuterShuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(1)
    val f = z.rightOuterShuffleRegionJoin(x, Some(1)).rdd.collect
    val g = x.rightOuterShuffleRegionJoin(x).rdd.collect

    val setDiff = f.toSet -- g.toSet
    assert(setDiff.isEmpty)
    println(f.length)
    assert(f.length == g.length)
  }

  sparkTest("testing that sorted leftOuterShuffleRegionJoin matches unsorted") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(1)
    val h = z.leftOuterShuffleRegionJoin(x, Some(1)).rdd
    val i = z.leftOuterShuffleRegionJoin(x).rdd

    val setDiff = h.collect.toSet -- i.collect.toSet
    assert(setDiff.isEmpty)
    assert(h.count == i.count)
  }

  sparkTest("testing that we can persist the sorted knowledge") {
    val x = sc.loadBam(getClass.getResource("/bqsr1.sam").getFile)
    val z = x.repartitionAndSort(16)
    val fileLocation = tmpLocation()
    val saveArgs = new JavaSaveArgs(fileLocation, asSingleFile = true)
    z.save(saveArgs, true)

    val t = sc.loadParquetAlignments(fileLocation)
    assert(t.sorted)
    assert(t.rdd.partitions.length == z.rdd.partitions.length)
  }
}
