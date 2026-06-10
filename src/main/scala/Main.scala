//import Spark
import org.apache.spark.sql.SparkSession
import java.io.FileNotFoundException
import org.json4s.ParserUtil.ParseException

object Main {
  
  //Create SparkSession in mode local
  val spark = SparkSession.builder()
  .appName("RedditNER")
  .master("local[*]")
  .getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten
    if(subscriptions.isEmpty){ //Error no tengo subscripciones
      println(s"Error: No valid subscriptions found");
      return //salgo del programa
    }

    //Load subscritpions in RDD
    val subscriptionsRDD = sc.parallelize(subscriptions)

    //Create Accumulators
    val downloadFeedSuccess = sc.longAccumulator("Feeds Success")   //Feeds descargados con exito
    val feedsFailed = sc.longAccumulator("Feeds Failed")           //Feeds que fallaron
    val postsDownloads = sc.longAccumulator("Posts Download")        //posts descargados en total
    val postsDiscard = sc.longAccumulator("Posts Discard")        //posts descartados por texto nulo o vacio

    // Download feeds and parse posts, manejo excepciones
    val downloadResults = subscriptionsRDD.flatMap { subscription =>
        val feedOpt = FileIO.downloadFeed(subscription.url)
        if (feedOpt.isEmpty) { //manejo de error desde File.IO
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          feedsFailed.add(1)
          Iterator.empty
        } else {
           downloadFeedSuccess.add(1)
          try{ //manejo de errores en el parseo
            val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
            postsDownloads.add(posts.length) 
            postsDiscard.add(posts.length - Analyzer.filterEmptyPosts(posts).length)
            val iterator : Iterator[Post] = posts.iterator
           iterator
          } catch {
            case e : ParseException =>
              println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
              Iterator.empty
          }
        }   
    }

    // Count feed successes/failures
    downloadResults.count()
    println(downloadFeedSuccess.value)
    println(feedsFailed.value)
    println(postsDownloads.value)
    println(postsDiscard.value)  
    

    // Filter empty posts
    val filteredPosts = downloadResults.filter { post =>
      post.title.nonEmpty &&
      post.selftext.nonEmpty &&
      post.selftext.trim.nonEmpty
    }

    val postsFiltered = postsDiscard.value  //cantidad de posts filtrados (los vacios)

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if ((postsDownloads.value - postsDiscard.value) > 0) totalChars / filteredPosts.count() else 0    //filteredPosts.nonEmpty por postsDownloads - postsDiscards que son RDD y length por count
 

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> downloadFeedSuccess.value.toInt, //lo paso a Int pues devuelve long y stats espera un Int
      "feedsFailed" -> feedsFailed.value.toInt,
      "postsSuccess" -> postsDownloads.value.toInt,
      "postsFailed" -> postsDiscard.value.toInt,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars" -> avgChars.toInt
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.count() == 0) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // countByValue ya devuelve un Map directamente
    val typeStats = allEntities
    .map(_.entityType)
    .countByValue()   // devuelve Map[String, Long], no necesita collect masivo
    .map { case (k, v) => k -> v.toInt }  //Paso de Long a Int

    val entityCounts = allEntities
    .map(_.text)
    .countByValue()
    .map { case (k, v) => k -> v.toInt }

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
}
