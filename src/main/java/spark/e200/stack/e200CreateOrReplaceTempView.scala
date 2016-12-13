package spark.openmining.stack

object e200CreateOrReplaceTempView {
	def main(args: Array[String]){
    val conf = new org.apache.spark.SparkConf()
      .setAppName("e200SessionJson")
      .setMaster("local")
    val sc = new org.apache.spark.SparkContext(conf)
    val spark = new org.apache.spark.sql.SQLContext(sc)
     
    val dfInput = spark.read.format("csv")
      .option("header","true")  // get Column Name
      .option("inferSchema","true")  // Column Type 추론 
      .load("data/GDPbyCountry.csv")
    System.out.print(dfInput)
    dfInput.show()
    dfInput.printSchema
    dfInput.createOrReplaceTempView("GDPbyCountry")
    val qDfInputNObs = spark.sql("select count(*) as NumOfObs from GDPbyCountry")
    qDfInputNObs.show()
    
    val qDfInputTopN = spark.sql("select * from GDPbyCountry limit 10")
    qDfInputTopN.show()
    qDfInputTopN.printSchema()

  }  
}
