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
        val titles = sc.textFile(args(0)).zipWithIndex().mapValues(x => x+1).map(_.swap).persist()
        val rocky = titles.filter( x => x._2 == "Rocky_Mountain_National_Park").collect()(0)
        
        //Save the titles that are only in surf
        //val regex = ".*[Ss][Oo][Aa][Pp].*".r
        val regex = ".*[Ss][Uu][Rr][Ff][Ii][Nn][Gg].*".r
        val titlesWSurf = titles.filter( x => regex.pattern.matcher(x._2).matches)
        
        //Create a list of the surf indicies
        val surfIndex = titlesWSurf.map( x => x._1).collect()
        
        //Read in the links textfile from HDFS
        val input = spark.read.textFile(args(1)).rdd

        //Split the input file so we have the file with links associated with it
        val links = input.map(s => (s.split(": ")(0), s.split(": ")(1).split(" +")))

        //Keep only the IDS that are in our surf index
        val surfLinks = links.filter{ case(k,v) => (surfIndex.contains(k.toInt)) }.cache()
        
        //Add our new page to the list of titles
        val newTitles = titlesWSurf.union( sc.parallelize( Seq( (rocky._1, rocky._2))))
        
        //Take the Highest ID in the new graph and add 1, this will be the ID we place all rank on
        val newID = newTitles.reduce( (x,y) => if(x._1 > y._1) x else y)._1.toInt +1
        
        //Create a list of link IDs that we will use as our fake pages
        import Array._
        val newPageIndex = range(newID + 1, newID + 1 + surfIndex.size*2)
        val webTitles = newTitles.union( sc.parallelize( newPageIndex.map( x => (x.toLong, "fake " + x.toString) ).toSeq) )
        
        //Add the fake pages to the links
        val webLinks = surfLinks.union( sc.parallelize( newPageIndex.map( x => (x.toString, Array[String]()) ).toSeq  ))

        //Add the target page to all links
        val webLinksWithTarget = webLinks.map( x => (x._1, x._2 :+ rocky._1) )
        
        //Add the spam page with links to all target pages
        val webLinksString = webLinksWithTarget.map( x => (x._1, x._2.map(x => x.toString)))
        val finalLinks = webLinksString.union(sc.parallelize( Seq( (rocky._1.toString, newPageIndex.map(x => x.toString ) ) )) )
        
        //Count total number of pages in corpus
        val NUM_OF_TOTAL_PAGES = finalLinks.count

        //v0 = 1/number of pages
        var ranks = finalLinks.mapValues( v => 1.0/ NUM_OF_TOTAL_PAGES)

        //Run for 25 iterations
        for(i <- 1 to 25){
            val tempRank = finalLinks.join(ranks).values.flatMap { case (urls, rank) =>
                val size = urls.size
                urls.map(url => (url, rank / size))
            }
            
            ranks  = tempRank.reduceByKey(_ + _) 
        }

        //Sort top 10 ranks in descending order
        val bestRanks = ranks.coalesce(1).sortBy(_._2, false).zipWithIndex.filter{ case(_, indx) => (indx < 10) }.keys

        val finalTitles = webTitles.map( x => (x._1.toString, x._2))
        val spam = finalTitles.join(bestRanks).map{ case(k, (ls, rs)) => (k, ls, rs) }.sortBy(_._3, false).coalesce(1)
        spam.saveAsTextFile(args(2))

        spark.stop()
    }
}
