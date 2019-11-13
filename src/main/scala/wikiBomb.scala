import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.{Row, SparkSession, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object wikiBomb {
    def main(args: Array[String]){
        
        //Create the spark session and the spark context
        val spark = SparkSession.builder.appName("SparkPageRank").getOrCreate()
        val sc = SparkContext.getOrCreate()
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        
        
        //Read in all the titles
        val titles = sc.textFile("hdfs://nashville:30841/cs435/pagerank/fake.txt").zipWithIndex().mapValues(x => x+1).map(_.swap).toDF("ID", "TITLES")

        //Save the titles that are only in surf
        val titlesWSurf = titles.filter($"TITLES" rlike ".*[Ss][Uu][Rr][Ff][Ii][Nn][Gg].*")
        
        //Create a list of the surf indicies
        val surfIndex = titlesWSurf.select("ID").rdd.map(r => r(0)).collect()
        
        //Read in the links textfile from HDFS
        val input = sc.textFile("hdfs://nashville:30841/cs435/pagerank/fnum.txt")
        
        //Split the input file so we have the file with links associated with it
        val links = input.map(s => (s.split(": ")(0), s.split(": ")(1).split(" +"))).toDF("ID", "LINKS")
        
        //Keep only the IDS that are in our surf index
        //val surfLinks = links.filter("$ID".isin(surfIndex:_*))
        val surfLinks = links.filter(col("ID").isin (surfIndex:_*))
        
        //Take the Highest ID in the new graph and add 1, this will be the ID we place all rank on
        val newID = surfLinks.agg(max(surfLinks.columns(0))).head()(0).toString.toInt + 1
        
        //Add our new page to the list of titles
        val newTitles = titlesWSurf.union( Seq( (newID.toString, "Rocky Mountain National Park") ).toDF() )
        
        //Create a list of link IDs that we will use as our fake pages
        import Array._
        val newPageIndex = range(newID + 1, newID + 1 + surfIndex.size*2)
        val newPageTitles = newPageIndex.map( x => (x, "fake " + x.toString) ).toSeq.toDF("ID", "TITLE")
        val webTitles = newTitles.union(newPageTitles)
        
        //Add the fake pages to the links
        val newPageLinks = newPageIndex.map( x => (x, Array[String]()) ).toSeq.toDF("ID", "LINKS")
        val webLinks = surfLinks.union(newPageLinks)
        
        //Add the target page to all links
        
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
        
        //Join the titles with ranks
        //val combined = titles.join(bestRanks, Seq("ID")).sort($"RANK".desc)
        //combined

        spark.stop()
    }
}
