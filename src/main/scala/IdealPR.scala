import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.{Row, SparkSession, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object IdealPR {
    def main(args: Array[String]){
        
        //If we have 4 args we are doing taxation else we are doing Ideal
        val taxation = (args.length == 4)
        
        //Create the spark session and the spark context
        val spark = SparkSession.builder.appName("SparkPageRank").getOrCreate()
        val sc = SparkContext.getOrCreate()
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        
        //Read in the links textfile from HDFS
        val input = spark.read.textFile(args(0)).rdd
        
        //Split the input file so we have the file with links associated with it
        val links = input.map(s => (s.split(": ")(0), s.split(": ")(1).split(" +")))
        //links.partitionBy(new HashPartitioner(100)).persist()
        
        //Count total number of pages in corpus
        val NUM_OF_TOTAL_PAGES = links.count

        //v0 = 1/number of pages
        var ranks = links.mapValues( v => 1.0/ NUM_OF_TOTAL_PAGES)

        //Run for 25 iterations
        for(i <- 1 to 25){
            val tempRank = links.join(ranks).values.flatMap { case (urls, rank) =>
                val size = urls.size
                urls.map(url => (url, rank / size))
            }
            
            if(taxation){
                ranks =  tempRank.reduceByKey(_ + _).mapValues( (0.15/NUM_OF_TOTAL_PAGES) + 0.85 * _)
            } else {
                ranks  = tempRank.reduceByKey(_ + _) 
            }
        }
        
        //Sort top 10 ranks in descending order
        //val sortedRanks = ranks.sortBy(_._2, false).toDF("ID", "RANK").limit(10)
        
        //val titles = sc.textFile(args(1)).zipWithIndex().mapValues(x => x+1).map(_.swap).toDF("ID", "TITLES")
        //val combined = titles.join(ranks.toDF("ID", "RANK"), Seq("ID")).sort($"RANK".desc).limit(10)
        
        val bestRanks = ranks.coalesce(1).toDF("ID", "RANK").sort($"RANK".desc).limit(10)
        bestRanks.rdd.saveAsTextFile(args(2))
        //bestRanks.rdd.saveAsTextFile(args(2))
        //Get the titles
        //val bestRanks = ranks.coalesce(1).sortBy(_._2, false).zipWithIndex.filter{ case(_, indx) => (indx < 10) }.keys.persist()
        //val rankID = bestRanks.map(x => x._1).collect()
        
        //val titles = sc.textFile(args(1)).zipWithIndex().mapValues(x => x+1).map(_.swap).filter{ case(k,v) => (rankID.contains(k.toString)) }

        //val finalTitles = titles.map( x => (x._1.toString, x._2))
        //val combined = finalTitles.join(bestRanks).map{ case(k, (ls, rs)) => (k, ls, rs) }.sortBy(_._3, false).coalesce(1)
        //combined.saveAsTextFile(args(2))
        
        //Join the titles with ranks
        //val combined = titles.join(bestRanks, Seq("ID")).sort($"RANK".desc)
        //combined

     //ROCKY MOUNTAIN -- 4290746
     spark.stop()
    }
}
