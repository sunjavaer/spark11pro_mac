package lvliang

import org.apache.spark.sql.SparkSession

/**
  * <p>Company:misspao </p >
  *
  * @author: lvliang
  * @Date: Create in 11:48 2019-05-23
  * @Description:
  */
object CutWordTest {

  def main(args: Array[String]): Unit = {
    print("cut word test")

    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    val orders = spark.sql("select * from misspao.mp_deposit_order")

    import org.apache.spark.sql.functions._
    import spark.implicits._
    orders.filter($"description".isNotNull)
      .show(5)

    spark.stop()
  }
}
