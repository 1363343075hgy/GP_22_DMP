package utils

trait Tage {
  /**
    * 打标签的统一接口
    */
  def makeTags(args:Any*):List[(String,Int)]
}