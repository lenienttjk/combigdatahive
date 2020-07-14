package com.black_unique.udf

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * @Auther: tjk
  * @Date: 2020-07-07 22:08
  * @Description:
  */

object Lat_lng_2_area extends UDF {

  def evaluate(lat: String, lng: String): String = {
    //判断传入的数据是否合法
    if (StringUtils.isEmpty(lat) || StringUtils.isEmpty(lng)) throw new RuntimeException("参数有误")
    // 传入参数
    val json = getJson(lat, lng)
    // 传入参数
    val res = getResult(json)
    // 返回结果
    res
  }

  /**
    * 传入url,返回字符串
    *
    * @param urlStr
    * @return
    */
  def getUrlResult(urlStr: String) = {

    // 获取 客户端
    val client = HttpClients.createDefault()
    val httpGet = new HttpGet(urlStr)

    // 发送请求，间隔1秒
    Thread.sleep(1000)

    val reponse = client execute httpGet
    // 格式化结果
    EntityUtils.toString(reponse.getEntity, "UTF-8")

  }

  /**
    * 传入经纬度，返回json传
    *
    * @param lat
    * @param lng
    * @return
    */
  def getJson(lat: String, lng: String): String = {

    // lat  维度：0-90，lng:经度：0-180
    val lat_lng = lat + "," + lng

    // 获取 url
    val urlStr = "https://apis.map.qq.com/ws/geocoder/v1/?location=" + lat_lng + "&key=IWABZ-PM362-K4XU4-CAMZY-4Y5CK-UJBNI" + "&get_poi=1"

    // 获取http，返回的是json
    val res = getUrlResult(urlStr)

    // 返回值:一行json数据
    res
  }

  /**
    * 传入json串，返回结果
    *
    * @param json
    * @return
    */
  def getResult(json: String): String = {
    // 解析 json
    val jsonObj: JSONObject = JSON.parseObject(json)

    // 判断状态
    val status = jsonObj.getIntValue("status")
    if (0 != status) {
      return ""
    }

    // 判断状态
    val resultArr = jsonObj.getJSONObject("result")
    if (resultArr == null) {
      return ""
    }

    // 判断状态
    val addre = resultArr.getJSONObject("address_component")
    if (addre == null) {
      return ""
    }

    val province = addre.getString("province")

    val city = addre.getString("city")
    val district = addre.getString("district")
    val street = addre.getString("street")

    val loca = resultArr.getJSONObject("location")
    val lat = loca.getString("lat")
    val lng = loca.getString("lng")

    val res = lat + "," + lng + "," + province + "," + city + "," + district + "," + street
    res

  }

  def main(args: Array[String]): Unit = {
    println(Lat_lng_2_area.evaluate("40.64178", "109.83859"))
  }
}