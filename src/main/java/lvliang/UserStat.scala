package lvliang

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

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

    val orders = spark.sql("select * from badou.orders").where("user_id <> 'user_id'")

    var toInt = udf((col: String) => if (col == null) {0} else if(col.isEmpty()) {0} else {col.toFloat})

    //获取每个用户的订单总量
    val userOrderCount = orders.groupBy("user_id").count().withColumnRenamed("count", "user_product_count")
    val userOrderSumDays = orders.withColumn("days_since_prior_order_int", toInt(col("days_since_prior_order"))).select("user_id", "days_since_prior_order_int").groupBy("user_id").sum("days_since_prior_order_int").withColumnRenamed("sum(days_since_prior_order_int)", "sum_order_day")

    var userOrderPerDays = udf((userProductCount:Int, sumUserOrderDay:Float) => (sumUserOrderDay.toFloat / userProductCount).formatted("%.2f"))
    userOrderCount.join(userOrderSumDays, "user_id").withColumn("user_Order_Per_Days", userOrderPerDays(col("user_product_count"), col("sum_order_day"))).show(10)

    spark.stop()
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

    orders.join(order_products_prior, "order_id")
      .select("user_id", "product_id")
      .distinct()
      .groupBy("user_id")
        .agg(concat_ws(",", collect_list(col("product_id"))).as("product_list"))
      .selectExpr("cast(user_id as int) as usr_id", "product_list")
      .orderBy("usr_id")
      .orderBy(desc("usr_id"))
      .show(5)

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
    //定义函数
    var userAvgProductCountOfOrder = udf((productCount:String, orderCount:String) => ((productCount.toFloat / orderCount.toFloat).formatted("%.2f")))

    userOrderCount.join(userProductCount, "user_id").withColumn("userAvgProductCountOfOrder", userAvgProductCountOfOrder(col("user_product_count"), col("user_order_count"))).show(20)

    spark.stop()

    spark.sql("select * from order").rollup("area", "memberType").agg(sum("price"))
  }

  def spartOrderStat() = {
    val warehouseLocation = "/usr/hive/warehouse"
    val spark: SparkSession = SparkSession.builder
      .appName("SparkTest")
      //      .master("local[*]")               //提交模式交给spark-submit控制
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate

    val url = "jdbc:mysql://47.105.211.70:3124/misspao_20190424?characterEncoding=utf8"
    val properties:Properties = new Properties
    properties.setProperty("user", "misspao")
    properties.setProperty("password", "misspao")
    val tableName = "mp_sport_order_stat"

    val file = spark.read.json("/misspao_data/order.json")
    file.createOrReplaceTempView("orders")

    val orders = spark.sql("select * from orders")
    val ordersResult = orders.selectExpr("year(createTime) as year", "cityName", "areaName", "status", "needPayAmount", "cast(newOrder as Int)")
      .rollup("year", "cityName", "areaName", "status")
      .agg(sum("needPayAmount").as("sum_needPayAmount"),
        count("needPayAmount").as("count_order"),
        sum("newOrder").as("count_new_order"))
      .orderBy("year", "cityName", "areaName", "status")

    //.write.mode(SaveMode.Overwrite).jdbc(url, tableName, properties)
    val result = ordersResult.selectExpr("cast(year as String)", "cityName", "areaName", "status", "sum_needPayAmount", "count_order", "count_new_order")

    result.selectExpr("if(year is null, 'all', year) as year", "cityName", "areaName", "status as payStatus", "sum_needPayAmount", "count_order", "count_new_order").write.mode(SaveMode.Overwrite).jdbc(url, tableName, properties)

  }

  def main(args: Array[String]): Unit = {

    UserStat.spartOrderStat()
  }

}
