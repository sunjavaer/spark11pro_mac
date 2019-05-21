package lvliang

import org.apache.spark.sql.SparkSession

/**
  * 提交命令：
  *
  * ./bin/spark-submit --class lvliang.SparkTest --master yarn --deploy-mode cluster --files /usr/local/src/apache-hive-1.2.2-bin/conf/hive-site.xml /opt/spark11pro_mac-1.0-jar-with-dependencies.jar
  */

object SparkTest {

  def main(args: Array[String]): Unit = {
    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
//      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    val orders = spark.sql("select * from badou.orders limit 20")

    orders.show(5)
  }
}