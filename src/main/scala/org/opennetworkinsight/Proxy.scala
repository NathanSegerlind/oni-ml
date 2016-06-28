package org.opennetworkinsight

import org.apache.log4j.{Level, Logger => apacheLogger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Contains routines for creating the "words" for a suspicious connects analysis from incoming DNS records.
  */
object Proxy {


  case class Config(inputPath: String = "")

  val parser = new scopt.OptionParser[Config]("Proxy Word Analysis") {

    head("Proxy Word Analysis", "Experimental")

    opt[String]('i', "input").required().valueName("<hdfs path>").
      action((x, c) => c.copy(inputPath = x)).
      text("HDFS path to netflow records")


  }

  def run(args: Array[String]) = {
    parser.parse(args.drop(1), Config()) match {

      case Some(config) => {
        val logger = LoggerFactory.getLogger(this.getClass)
        apacheLogger.getLogger("org").setLevel(Level.OFF)
        apacheLogger.getLogger("akka").setLevel(Level.OFF)

        println("Proxy Word Analysis:  START")

        println("Reading data from HDFS path: " + config.inputPath)
        val conf = new SparkConf().setAppName("Proxy Word Analysis")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        /*
        var time_cuts = new Array[Double](10)
        var frame_length_cuts = new Array[Double](10)
        var subdomain_length_cuts = new Array[Double](5)
        var numperiods_cuts = new Array[Double](5)
        var entropy_cuts = new Array[Double](5)

        */


        var df_cols = new Array[String](0)

        val rawdata = {
          var df = sqlContext.parquetFile(config.inputPath.split(",")(0)).filter("proxy_date is not null and proxy_time is not null and proxy_clientip is not null")
          val files = config.inputPath.split(",")
          for ((file, index) <- files.zipWithIndex) {
            if (index > 1) {
              df = df.unionAll(sqlContext.parquetFile(file).filter("proxy_date is not null and proxy_time is not null and proxy_clientip is not null"))
            }
          }
          df = df.select("proxy_date", "proxy_time", "proxy_clientip", "proxy_host", "proxy_reqmethod",
            "proxy_useragent", "proxy_resconttype", "proxy_respcode", "proxy_fulluri")
          df_cols = df.columns
          val tempRDD: org.apache.spark.rdd.RDD[String] = df.map(_.mkString(","))
          tempRDD
        }

        println("Read source data")


        val col = DNSWordCreation.getColumnNames(df_cols)

        def addcol(colname: String) = if (!col.keySet.exists(_ == colname)) {
          col(colname) = col.values.max + 1
        }

        val datagood = rawdata.map(line => line.split(",")).filter(line => (line.length == df_cols.length))

        println("Columns: " + df_cols.mkString(" "))

        val queryCount = datagood.count()
        println("Total lines of source data read: " + queryCount)


        val distinctClients = datagood.map(r => r(2)).distinct()
        val distinctClientCount = distinctClients.count()
        println("Distinct clients: "  + distinctClientCount)
        println("Typical client:" + distinctClients.take(1)(0))

        val distinctHosts = datagood.map(r => r(3)).distinct()
        val distinctHostCount = distinctHosts.count()
        println("Distinct hosts: "  + distinctHostCount)
        println("Typical host:" + distinctHosts.take(1)(0))


        val distinctReqMethods = datagood.map(r => r(4)).distinct()
        val distinctReqMethodCount = distinctReqMethods.count()
        println("Distinct req methods: "  + distinctReqMethodCount)
        val reqMethods = distinctReqMethods.collect()
        println("Req methods: " + reqMethods.mkString(" "))


        val distinctUserAgents = datagood.map(r => r(5)).distinct()
        val distinctUserAgentsCount = distinctUserAgents.count()
        println("Distinct user agents: "  + distinctUserAgentsCount)
        println("Typical user agent:" + distinctUserAgents.take(1)(0))


        val distinctContentTypes = datagood.map(r => r(6)).distinct()
        val distinctContentTypeCount = distinctContentTypes.count()
        println("Distinct content types: "  + distinctContentTypeCount)
        val contentTypeCounts = datagood.map(r=>(r(6), 1)).reduceByKey(_+_).collect().sortWith({case ((_,c1), (_, c2)) => c1 > c2}) // swapped because we want a reverse order with top in front
        val outString = contentTypeCounts.take(100).map({case (s,c) => s + " : " + c }).mkString("\n")
        println("Top 100 content types: " + outString)


        val distinctRespCodes = datagood.map(r => r(7)).distinct()
        val distinctRespCodeCount = distinctRespCodes.count()
        println("Distinct response codes: "  + distinctRespCodeCount)
        println("Typical response codes:" + distinctRespCodes.take(1)(0))

        val distinctFullURI = datagood.map(r => r(8)).distinct()
        val distinctFullURICount = distinctFullURI.count()
        println("Distinct full URIs: "  + distinctFullURICount)
        println("Typical full URI:" + distinctFullURI.take(1)(0))

        println("Imma gon collect your full URIs now: ")
        val uris  = distinctFullURI.collect()

        println("See that didn't hurt")


/*

        var data_with_subdomains = datagood.map(row => row ++ DNSWordCreation.extractSubdomain(country_codes, row(col("dns_qry_name"))))
        addcol("domain")
        addcol("subdomain")
        addcol("subdomain.length")
        addcol("num.periods")

        data_with_subdomains = data_with_subdomains.map(data => data :+ DNSWordCreation.entropy(data(col("subdomain"))).toString)
        addcol("subdomain.entropy")

        logger.info("Calculating time cuts ...")
        time_cuts = Quantiles.computeDeciles(data_with_subdomains.map(r => r(col("unix_tstamp")).toDouble))
        logger.info(time_cuts.mkString(","))



        var data = data_with_subdomains.map(line => line :+ {
          if (line(col("domain")) == "intel") {
            "2"
          } else if (top_domains.value contains line(col("domain"))) {
            "1"
          } else "0"
        })
        addcol("top_domain")

        logger.info("Adding words")
        data = data.map(row => {
          val word = row(col("top_domain")) + "_" + DNSWordCreation.binColumn(row(col("frame_len")), frame_length_cuts) + "_" +
            DNSWordCreation.binColumn(row(col("unix_tstamp")), time_cuts) + "_" +
            DNSWordCreation.binColumn(row(col("subdomain.length")), subdomain_length_cuts) + "_" +
            DNSWordCreation.binColumn(row(col("subdomain.entropy")), entropy_cuts) + "_" +
            DNSWordCreation.binColumn(row(col("num.periods")), numperiods_cuts) + "_" + row(col("dns_qry_type")) + "_" + row(col("dns_qry_rcode"))
          row :+ word
        })
        addcol("word")

        logger.info("Persisting data")
        val wc = data.map(row => (row(col("ip_dst")) + " " + row(col("word")), 1)).reduceByKey(_ + _).map(row => (row._1.split(" ")(0) + "," + row._1.split(" ")(1).toString + "," + row._2).mkString)
        wc.persist(StorageLevel.MEMORY_AND_DISK)
        wc.saveAsTextFile(config.outputPath)
*/

        sc.stop()
        logger.info("DNS pre LDA completed")
      }

      case None => println("Error parsing arguments")
    }

  }
}

