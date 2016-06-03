package main.scala

import main.scala.DNSTransformation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import scala.io.Source

object DnsPreLDA {

    def run() = {

        val conf = new SparkConf().setAppName("ONI ML: dns pre lda")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val file_list = System.getenv("DNS_PATH")
        val feedback_file = "None"
        val duplication_factor = 100
        val outputfile = System.getenv("HPATH") + "/word_counts"
        var time_cuts = new Array[Double](10)
        var frame_length_cuts = new Array[Double](10)
        var subdomain_length_cuts = new Array[Double](5)
        var numperiods_cuts = new Array[Double](5)
        var entropy_cuts = new Array[Double](5)
        var df_cols = new Array[String](0)

        val l_top_domains = Source.fromFile("top-1m.csv").getLines.map(line => {
            val parts = line.split(",")
            val l = parts.length
            parts(1).split("[.]")(0)
        }).toSet
        val top_domains = sc.broadcast(l_top_domains)

        val multidata = {
            var df = sqlContext.parquetFile(file_list.split(",")(0)).filter("frame_len is not null and unix_tstamp is not null")
            val files = file_list.split(",")
            for ((file, index) <- files.zipWithIndex) {
                if (index > 1) {
                    df = df.unionAll(sqlContext.parquetFile(file).filter("frame_len is not null and unix_tstamp is not null"))
                }
            }
            df = df.select("frame_time", "unix_tstamp", "frame_len", "ip_dst", "dns_qry_name",
                            "dns_qry_class", "dns_qry_type", "dns_qry_rcode")
            df_cols = df.columns
            val tempRDD: org.apache.spark.rdd.RDD[String] = df.map(_.mkString(","))
            tempRDD
        }

        val rawdata: org.apache.spark.rdd.RDD[String] = {
            if (feedback_file == "None") {
                multidata
            } else {
                var data: org.apache.spark.rdd.RDD[String] = multidata
                val feedback: org.apache.spark.rdd.RDD[String] = sc.textFile(feedback_file)
                val falsepositives = feedback.filter(line => line.split(",").last == "3")
                var i = 1
                while (i < duplication_factor) {
                    data = data.union(falsepositives)
                    i = i + 1
                }
                data
            }
        }

        val col = DNSTransformation.getColumnNames(df_cols)

        def addcol(colname: String) = if (!col.keySet.exists(_ == colname)) {
            col(colname) = col.values.max + 1
        }
        if (feedback_file != "None") {
            addcol("feedback")
        }

        val datagood = rawdata.map(line => line.split(",")).filter(line => (line.length == df_cols.length)).map(line => {
            if (feedback_file != "None") {
                line :+ "None"
            } else {
                line
            }
        })

        val country_codes = sc.broadcast(DNSTransformation.l_country_codes)

        println("Computing subdomain info")

        var data_with_subdomains = datagood.map(row => row ++ DNSTransformation.extractSubdomain(country_codes, row(col("dns_qry_name"))))
        addcol("domain")
        addcol("subdomain")
        addcol("subdomain.length")
        addcol("num.periods")

        data_with_subdomains = data_with_subdomains.map(data => data :+ DNSTransformation.entropy(data(col("subdomain"))).toString)
        addcol("subdomain.entropy")

        println("calculating time cuts ...")
        time_cuts = Quantiles.distributedQuantilesQuant(Quantiles.computeEcdf(data_with_subdomains.map(r => r(col("unix_tstamp")).toDouble)))
        println(time_cuts.mkString(","))

        println("calculating frame length cuts ...")
        frame_length_cuts = Quantiles.distributedQuantilesQuant(Quantiles.computeEcdf(data_with_subdomains.map(r => r(col("frame_len")).toDouble)))
        println(frame_length_cuts.mkString(","))
        println("calculating subdomain length cuts ...")
        subdomain_length_cuts = Quantiles.distributedQuantilesQuint(Quantiles.computeEcdf(data_with_subdomains.filter(r => r(col("subdomain.length")).toDouble > 0).map(r => r(col("subdomain.length")).toDouble)))
        println(subdomain_length_cuts.mkString(","))
        println("calculating entropy cuts")
        entropy_cuts = Quantiles.distributedQuantilesQuint(Quantiles.computeEcdf(data_with_subdomains.filter(r => r(col("subdomain.entropy")).toDouble > 0).map(r => r(col("subdomain.entropy")).toDouble)))
        println(entropy_cuts.mkString(","))
        println("calculating num periods cuts ...")
        numperiods_cuts = Quantiles.distributedQuantilesQuint(Quantiles.computeEcdf(data_with_subdomains.filter(r => r(col("num.periods")).toDouble > 0).map(r => r(col("num.periods")).toDouble)))
        println(numperiods_cuts.mkString(","))

        var data = data_with_subdomains.map(line => line :+ {
            if (line(col("domain")) == "intel") {
                "2"
            } else if (top_domains.value contains line(col("domain"))) {
                "1"
            } else "0"
        })
        addcol("top_domain")

        println("adding words")
        data = data.map(row => {
            val word = row(col("top_domain")) + "_" + DNSTransformation.binColumn(row(col("frame_len")), frame_length_cuts) + "_" +
              DNSTransformation.binColumn(row(col("unix_tstamp")), time_cuts) + "_" +
              DNSTransformation.binColumn(row(col("subdomain.length")), subdomain_length_cuts) + "_" +
              DNSTransformation.binColumn(row(col("subdomain.entropy")), entropy_cuts) + "_" +
              DNSTransformation.binColumn(row(col("num.periods")), numperiods_cuts) + "_" + row(col("dns_qry_type")) + "_" + row(col("dns_qry_rcode"))
            row :+ word
        })
        addcol("word")

        val wc = data.map(row => (row(col("ip_dst")) + " " + row(col("word")), 1)).reduceByKey(_ + _).map(row => (row._1.split(" ")(0) + "," + row._1.split(" ")(1).toString + "," + row._2).mkString)
        wc.persist(StorageLevel.MEMORY_AND_DISK)
        wc.saveAsTextFile(outputfile)

        sc.stop()

    }
}
