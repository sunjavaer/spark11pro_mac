package lvliang

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * <p>Company:misspao </p >
  *
  * @author: lvliang
  * @Date: Create in 11:50 2019-05-23
  * @Description:
  */
object UDFTest {


  def main(args: Array[String]): Unit = {
    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    val orders = spark.sql("select * from badou.orders limit 100")

    val order_dow = udf((col: String) =>
      if(col.equals("0")) {
        return "MON"
      } else {
        return "none"
      }
    )

    orders.withColumn("days", order_dow(col("order_dow"))).show()
  }
}
