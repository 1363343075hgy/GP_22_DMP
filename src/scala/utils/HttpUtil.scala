package utils

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client. HttpClients
import org.apache.http.util.EntityUtils

object HttpUtil {
      //GET请求
  def get(url:String):String={
      val client = HttpClients.createDefault()
      val geturl = new HttpGet(url)
      //发送请求
      val response: CloseableHttpResponse = client.execute(geturl)
      //获取返回结果
      EntityUtils.toString(response.getEntity,"UTF-8")
  }
}
