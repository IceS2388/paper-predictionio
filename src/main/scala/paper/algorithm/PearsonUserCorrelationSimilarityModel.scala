package paper.algorithm

import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity


/**
  * Pearson用户相似度模板
  * */
class PearsonUserCorrelationSimilarityModel(
   val rank: Int,
   val abSimilarity: PearsonCorrelationSimilarity) {

}
