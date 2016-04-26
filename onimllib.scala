package onimllib {
  def compute_ecdf (x: org.apache.spark.rdd.RDD[Double] ): org.apache.spark.rdd.RDD[(Double, Double)] = {
  val counts = x.map (v => (v, 1) ).reduceByKey (_+ _).sortByKey ().cache ()
  // compute the partition sums
  val partSums: Array[Double] = 0.0 +: counts.mapPartitionsWithIndex {
  case (index, partition) => Iterator (partition.map {
  case (sample, count) => count
}

.sum.toDouble)}.collect ()

// get sample size
val numValues = partSums.sum

// compute empirical cumulative distribution
val sumsRdd = counts.mapPartitionsWithIndex {
case (index, partition) => {
var startValue = 0.0
for (i <- 0 to index) {
startValue += partSums (i)
}
partition.scanLeft ((0.0, startValue) ) ((prev, curr) => (curr._1, prev._2 + curr._2) ).drop (1)
}
}
sumsRdd.map (elem => (elem._1, elem._2 / numValues) )
}
}

