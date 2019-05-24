package lvliang

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * <p>Company:misspao </p >
  *
  * @author: lvliang
  * @Date: Create in 20:18 2019-05-24
  * @Description:
  */
object Product01 {

  /**
    * 统计product被购买的数据量
    */
  def q1() = {
    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    val order_products_prior = spark.sql("select * from badou.order_products_prior")
    //    order_products_prior.groupBy(col("product_id")).agg(col("order_id")).show(10)


    order_products_prior.groupBy("product_id").count().show(5)

    spark.stop()
  }

  /**
    * 统计product 被reordered的数量（再次购买）
    */
  def q2() = {
    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    val order_products_prior = spark.sql("select * from badou.order_products_prior")

    order_products_prior.where("reordered = 1").groupBy("product_id").count().show(5)

    spark.stop()
  }

  /**
    * 结合上面数量统计product购买的reordered 的比率
    */
  def q3() = {
    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    val order_products_prior = spark.sql("select * from badou.order_products_prior")
    order_products_prior.cache()

    val productCount = order_products_prior.groupBy("product_id").count().toDF("product_id","product_count")

    val productBuyCount = order_products_prior.where("reordered = 1").groupBy("product_id").count().toDF("product_id","product_buy_count")

    var buyRatio = udf((col1:String,col2:String)=> ((col1.toFloat / col2.toFloat) * 100).formatted("%.2f"))

    productBuyCount.join(productCount, "product_id").withColumn("buy_ratio", buyRatio(col("product_buy_count"), col("product_count"))).show(100)

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
//    Product01.q1()
//    Product01.q2()
    Product01.q3()
  }

}
