import org.apache.spark.{SparkConf, SparkContext}

object MyFirstScala {

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("mySpark").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("E://hello.txt")
    file.flatMap(line => line.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
      .foreach(println)



  }

}
