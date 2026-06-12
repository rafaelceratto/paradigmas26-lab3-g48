// Importamos las librerías necesarias para Spark y los RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
  
    // PARSEO DE ARGUMENTOS
  
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // Si están mal, el programa termina acá
    }
    // INICIALIZACIÓN DE SPARK
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()

    // El SparkContext es nuestra puerta de entrada para crear RDDs
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")
    //LECTURA SEGURA DE ARCHIVOS (Requisito del Ejercicio 2)
    
    val subscriptions = try {
      val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)
      val subs = subscriptionOpts.flatten // Limpiamos los None (malformados)
      
      // Si el archivo JSON existe pero no tiene suscripciones adentro
      if (subs.isEmpty) {
        println("Error: No valid subscriptions found")
        spark.stop()
        sys.exit(1)
      }
      subs // Devolvemos la lista limpia
      
    } catch {
      // Si el archivo JSON no se encuentra en la ruta
      case _: java.io.FileNotFoundException =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} - file not found")
        spark.stop()
        sys.exit(1)
      
      // Si el archivo JSON existe pero está mal escrito (error de sintaxis)
      case _: Exception => 
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} - invalid JSON format")
        spark.stop()
        sys.exit(1)
    }

    // Validamos que exista la carpeta de diccionarios
    val entitiesDirFile = new java.io.File(cmdArgs.entitiesDir)
    if (!entitiesDirFile.exists() || !entitiesDirFile.isDirectory) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      spark.stop()
      sys.exit(1)
    }

    // Ejercicios 2 y 4
    
    val feedsSuccessAcc = sc.longAccumulator("FeedsDescargadosConExito")
    val feedsFailedAcc = sc.longAccumulator("FeedsFallidos")
    val postsDownloadedTotalAcc = sc.longAccumulator("TotalPostsDescargados")
    val postsDiscardedAcc = sc.longAccumulator("PostsDescartadosVacios")

    val suscriptionsRDD = sc.parallelize(subscriptions)

    // Usamos flatMap porque cada Worker va a descargar una URL y obtener una LISTA de posts.
    // Si usáramos 'map', nos quedaría un RDD de listas ( RDD[List[Post]] ).
    // flatMap "aplana" todo eso y nos devuelve un único RDD gigante de posts ( RDD[Post] ).
    val postsRDD: RDD[Post] = suscriptionsRDD.flatMap { s =>
      
      //Descargamos el feed (Esto se ejecuta adentro de cada Worker)
      val feedOpt: Option[String] = FileIO.downloadFeed(s.url) 
      
      //Analizamos el resultado de la descarga
      val postsDelFeed: List[Post] = feedOpt match {
        case Some(htmlContent) =>
          // Si fue exitoso, sumamos 1 al acumulador de éxito y parseamos el JSON
          feedsSuccessAcc.add(1)
          JsonParser.parsePosts(htmlContent, s.name)
          
        case None =>
          // Si falló (timeout, red rota), sumamos al de fallas e imprimimos el warning
          feedsFailedAcc.add(1)
          println(s"Warning: Failed to download from '${s.name}' (${s.url})")
          // Devolvemos lista vacía para que este worker no aporte nada y el programa siga
          List.empty[Post]
      }
      
      // Sumamos al acumulador la cantidad de posts CRUDOS que descargó este feed
      postsDownloadedTotalAcc.add(postsDelFeed.length)
      
      // 3. Filtramos los posts que están vacíos (sin título o sin texto)
      val validPosts = postsDelFeed.filter(p => p.title.nonEmpty && p.selftext.nonEmpty)
      
      // Calculamos cuántos descartamos y lo sumamos al acumulador correspondiente
      val descartados = postsDelFeed.length - validPosts.length
      postsDiscardedAcc.add(descartados)
      
      // Devolvemos solo los posts válidos para que flatMap los una al RDD final
      validPosts
    }

    // EJERCICIO 5
    postsRDD.cache()

    // Tomamos el tiempo justo antes de arrancar
    val startTime1 = System.currentTimeMillis()
    
    val localPosts: Array[Post] = postsRDD.collect()
    
    val endTime1 = System.currentTimeMillis()
    val duration1 = (endTime1 - startTime1) / 1000.0 

    if (localPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop() 
      sys.exit(1) 
    }
    // Calculamos el promedio de caracteres en el Driver usando los posts recolectados
    val totalChars = localPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (localPosts.nonEmpty) totalChars / localPosts.length else 0

    // Armamos las estadísticas usando '.value' para extraer el número de cada acumulador
    val stats = Map(
      "feedsSuccess"  -> feedsSuccessAcc.value.toInt,
      "feedsFailed"   -> feedsFailedAcc.value.toInt,
      "postsSuccess"  -> localPosts.length, // Estos son los posts válidos que sobrevivieron
      "postsFailed"   -> 0, // En nuestro Spark, lo que falla devuelve lista vacía y no suma error de parseo
      "postsFiltered" -> postsDiscardedAcc.value.toInt,
      "avgChars"      -> avgChars.toInt
    )
    // Usamos el formato que pide el esqueleto
    println(Formatters.formatProcessingStats(stats))
  
    println(s"\nTiempo de descarga y filtrado (Acción Terminal 1): $duration1 segundos\n")

    // EJERCICIO 3 
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // flatMap: Extraemos las entidades de todos los posts.
    val entitiesRDD = postsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // map: Convertimos cada entidad en un par clave-valor: ((Tipo, Nombre), 1)
    // Ejemplo: (("PERSONA", "Lionel Messi"), 1)
    val mapRDD = entitiesRDD.map { entity =>
      ((entity.entityType, entity.text), 1)
    }
    // c) reduceByKey: Sumamos los '1' de las entidades iguales.
    // Spark junta todas las claves iguales que andan desparramadas por los workers y las suma.
    val countsRDD = mapRDD.reduceByKey((a, b) => a + b)

    val startTime2 = System.currentTimeMillis()

    // Al hacer collect(), ordenamos a los workers que hagan el map-reduce
    val sortedEntities = countsRDD.sortBy { case ((tipo, nombre), conteo) =>
      (-conteo, tipo) // El '-' es para que ordene de mayor a menor (descendente)
    }.collect()

    val endTime2 = System.currentTimeMillis()
    val duration2 = (endTime2 - startTime2) / 1000.0

    println(s"Tiempo de Map-Reduce (Acción Terminal 2): $duration2 segundos\n")
    println("=== RANKING DE ENTIDADES (MAP-REDUCE) ===")
    
    // Mostramos solo el Top K (las mejores) como pide el enunciado y el formato exacto
    sortedEntities.take(cmdArgs.topK).foreach { case ((tipo, nombre), conteo) =>
      println(s"[$tipo] $nombre: $conteo apariciones")
    }
    // EJERCICIO 5: Liberamos la memoria caché ya que no usamos más el RDD
    postsRDD.unpersist()
    spark.stop()
  } 
}