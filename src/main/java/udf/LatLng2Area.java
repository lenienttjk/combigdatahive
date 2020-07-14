package udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class LatLng2Area extends UDF {

    public String evaluate(String lat, String lng) {

        // 传入参数
        String json = null;
        try {
            json = getJson(lat, lng);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 传入参数
        String res = getResult(json);
        // 返回结果
        return res;
    }

    public String getJson(String lat, String lng) throws IOException, InterruptedException {
        // lat  维度：0-90，lng:经度：0-180
        String lng_lat = lng + "," + lat;

        // 获取 url 腾讯 api 使用的是纬经度 （维度，经度）
//        String urlStr = "https://apis.map.qq.com/ws/geocoder/v1/?location=" + lat_lng + "&key=IWABZ-PM362-K4XU4-CAMZY-4Y5CK-UJBNI" + "&get_poi=1";

     // 高德地图 api 使用的是经纬度 （经度，维度）
        String urlStr = "https://restapi.amap.com/v3/geocode/regeo?location=" + lng_lat + "&key=a19f03acd91d1b995fe5dbb9cf069ccd" + "&radius=1000&extensions=all";

//https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=a19f03acd91d1b995fe5dbb9cf069ccd&radius=1000&extensions=all

        // 获取http，返回的是json
        String res = getUrlResult(urlStr);

        return res;
    }

    public String getUrlResult(String urlStr) throws InterruptedException, IOException {
        // 获取 客户端
        CloseableHttpClient cli = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(urlStr);

        // 发送请求，间隔1秒
        Thread.sleep(1000);


        CloseableHttpResponse reponse = null;
        try {
            reponse = cli.execute(httpGet);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 格式化结果

        return EntityUtils.toString(reponse.getEntity(), "UTF-8");


    }



    public String getResult(String json) {

        // 解析 json
        JSONObject jsonObj = JSON.parseObject(json);

        // 判断状态
        int status = jsonObj.getIntValue("status");
        if (1 != status) {
            return "";
        }

        // 判断状态
        JSONObject resultArr = jsonObj.getJSONObject("regeocode");
        if (resultArr == null) {
            return "";
        }

        // 判断状态
        JSONObject addre = resultArr.getJSONObject("addressComponent");
        if (addre == null) {
            return "";
        }

        String province = addre.getString("province");
        String city = addre.getString("city");
        String district = addre.getString("district");
        String township = addre.getString("township");

//        JSONObject loca = resultArr.getJSONObject("location");
//        String lat = loca.getString("lat");
//        String lng = loca.getString("lng");

        String res =   province + "," + city + "," + district + "," + township;
        return res;
    }


//    public static void main(String[] args) throws IOException, InterruptedException {
//        System.out.println(new LatLng2Area().evaluate("31.4141", "118.233131"));
//    }

}