package lvliang

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * <p>Company:misspao </p >
  *
  * @author: lvliang
  * @Date: Create in 09:58 2019-05-25
  * @Description:
  */
object UserStat {

  /**
    * 每个用户平均购买订单的间隔周期
    */
  def q1() = {
    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    val orders = spark.sql("select * from orders")

    //获取每个用户的订单总量
    val userOrderCount = orders.groupBy("user_id").count().withColumnRenamed("count", "user_order_count")

//    orders.select("days_since_prior_order").toDF().
    orders.where("user_id = 1").groupBy("user_id").sum("days_since_prior_order")
  }

  /**
    * 每个用户的总订单数量
    */
  def q2() = {
    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    var orders = spark.sql("select * from badou.orders")
    orders.groupBy("user_id").count().show(5)

    spark.stop()
  }

  /**
    * 每个用户购买的product商品去重后的集合数据
    */
  def q3() = {
    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    var orders = spark.sql("select * from badou.orders")
    var order_products_prior = spark.sql("select * from badou.order_products_prior")
      orders.join(order_products_prior, "order_id").select("user_id", "product_id").distinct().orderBy("user_id").show(5)

    spark.stop()
  }

  /**
    * 每个用户总商品数量以及去重后的商品数量
    */
  def q4() = {
    var warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    var orders = spark.sql("select * from badou.orders")
    var order_products_prior = spark.sql("select * from badou.order_products_prior")

    //获取用户总商品数量
    var userProductAllCount = orders.join(order_products_prior, "order_id").groupBy("user_id").count().toDF("user_id", "user_product_all_count")
    var userProudctCount = orders.join(order_products_prior, "order_id").select("user_id", "product_id").distinct().groupBy("user_id").count().toDF("user_id", "user_product_count")

    userProudctCount.join(userProductAllCount, "user_id").show(10)

    spark.stop()
  }

  /**
    * 每个用户购买的平均每个订单的商品数量（hive已做过）
    */
  def q5() = {
    val warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    val orders = spark.sql("select * from orders")
    val order_products_prior = spark.sql("select * from order_products_prior")

    //获取用户订单数量
    val userOrderCount = orders.groupBy("user_id").count().withColumnRenamed("count", "user_order_count")
    //获取用户购买商品数量
    val userProductCount = orders.join(order_products_prior, "order_id").groupBy("user_id").count().withColumnRenamed("count", "user_product_count")

    var userAvgProductCountOfOrder = udf((productCount:String, orderCount:String) => ((productCount.toFloat / orderCount.toFloat).formatted("%.2f")))

    userOrderCount.join(userProductCount, "user_id").withColumn("userAvgProductCountOfOrder", userAvgProductCountOfOrder(col("user_product_count"), col("user_order_count"))).show(20)

    spark.stop()
  }

  def main(args: Array[String]): Unit = {

    UserStat.q4()
  }

}
