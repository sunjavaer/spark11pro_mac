package lvliang

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


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

    def jieba_seg(df:DataFrame,colname:String): DataFrame ={

      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)
      val jieba_udf = udf{(sentence:String)=>
        val segV = seg.value
        segV.process(sentence.toString, SegMode.INDEX)
          .toArray().map(_.asInstanceOf[SegToken].word)
          .filter(_.length>1).mkString("/")
      }
      df.withColumn("cut_description",jieba_udf(col(colname)))
    }

    val orders = spark.sql("select * from misspao.mp_deposit_order")


    val description = orders.where("description <> 'null'")
      .where("description <> ''")
      .select("description")

    jieba_seg(description, "description").show(10)

    spark.stop()
  }
}
