package spark.e200.stack

object e200CreateOrReplaceTempView {
	def main(args: Array[String]){
    val conf = new org.apache.spark.SparkConf()
      .setAppName("e200SessionJson")
      .setMaster("local")
    val sc = new org.apache.spark.SparkContext(conf)
    val spark = new org.apache.spark.sql.SQLContext(sc)
    
    // Column Type 추론    

    val dfInput = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("data/TitanicTrain.csv")
    System.out.print(dfInput)
    dfInput.show()
    dfInput.printSchema
    dfInput.createOrReplaceTempView("TitanicTrain")
    val qDfInputNObs = spark.sql("select count(*) as NumOfObs from TitanicTrain")
    qDfInputNObs.show()
    
    val qDfInputTopN = spark.sql("select * from TitanicTrain limit 10")
    qDfInputTopN.show()
    qDfInputTopN.printSchema()

  }  
}
