package lvliang

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * <p>Company:misspao </p >
  *
  * @author: lvliang
  * @Date: Create in 10:49 2019-05-24
  * @Description:
  */
object JiebaKry2 {

  def main(args: Array[String]): Unit = {
    //    定义结巴分词类的序列化
    val conf = new SparkConf()
      .set("spark.rpc.message.maxSize","800")  //多个和conf相关的设置，比如优化参数，可以通过set传入进入
    //也可以将参数放入run脚本当中，避免重复打包
    //    建立sparkSession,并传入定义好的Conf
    val spark = SparkSession
      .builder()
      .appName("Jieba UDF")
      .enableHiveSupport()
      .config(conf)   //将conf放入config当中
      .getOrCreate()

    // 定义结巴分词的方法，传入的是DataFrame，输出也是DataFrame多一列seg（分好词的一列）
    // main函数里面也能定义函数
    def jieba_seg(df:DataFrame,colname:String): DataFrame ={
      val jieba_udf = udf{(sentence:String)=>                                 //定义udf函数，此处开始是task执行
        val segmenter = new JiebaSegmenter()   //实例化一个JiebaSegmenter类
        segmenter.process(sentence.toString,SegMode.INDEX)
          .toArray().map(_.asInstanceOf[SegToken].word)
          .filter(_.length>1).mkString("/")
      }
      df.withColumn("seg",jieba_udf(col(colname)))  //默认最后一行是返回值，这里还是driver执行
    }

    val df =spark.sql("select sentence, label from news_noseg limit 300")
    val df_seg = jieba_seg(df,"sentence")
    df_seg.show()
    df_seg.write.mode("overwrite").saveAsTable("news_jieba")  //将新数据保存到一个新表
  }

}
