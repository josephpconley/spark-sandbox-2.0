package app.avro

import util.BaseDriver
import com.databricks.spark.avro._

object AvroReader extends BaseDriver {

  override def run(): Unit = {
//    val services = spark.read.avro("/home/jconley/Downloads/part-m-00001.avro")
    val services = spark.read.format("com.databricks.spark.avro").load(
      "/home/jconley/Downloads/part-m-00001.avro",
      "/home/jconley/Downloads/part-m-00002.avro",
      "/home/jconley/Downloads/part-m-00003.avro"
    )

    services.cache()

    services.show()
    println(services.count())

    services.where("SERVICE_ID = 'MH0000061167'").show()
  }
}
