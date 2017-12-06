package NLP

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 11/27/17.
  */
object Word2VecExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word2VecExample").setMaster("local")
    val sc = new SparkContext(conf)

    // $example on$
    val input = sc.textFile("/root/spark-2.1.1/data/mllib/sample_lda_data.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("1", 5)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = Word2VecModel.load(sc, "myModelPath")
    // $example off$

    sc.stop()
  }
}
