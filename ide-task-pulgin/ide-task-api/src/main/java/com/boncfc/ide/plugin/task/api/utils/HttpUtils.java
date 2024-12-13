package com.boncfc.ide.plugin.task.api.utils;

import com.boncfc.ide.plugin.task.api.constants.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
/**
 * http 工具类
 */
@Slf4j
public class HttpUtils {

    /**
     * HTTP GET
     *
     * @param url
     * @return http response
     */
    public static String get(String url) {
        CloseableHttpClient httpclient = HttpClients.createDefault();

        HttpGet httpget = new HttpGet(url);
        //设置超时
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(Constants.HTTP_CONNECT_TIMEOUT)
                .setConnectionRequestTimeout(Constants.HTTP_CONNECTION_REQUEST_TIMEOUT)
                .setSocketTimeout(Constants.SOCKET_TIMEOUT)
                .setRedirectsEnabled(true)
                .build();
        httpget.setConfig(requestConfig);
        String responseContent = null;
        CloseableHttpResponse response = null;

        try {
            response = httpclient.execute(httpget);
            //检查响应状态是否是200
            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    responseContent = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                } else {
                    log.warn("http 响应体为空");
                }
            } else {
                log.error("http 响应状态码非200! 实际状态码: [{}]", response.getStatusLine().getStatusCode());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (response != null) {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }

            if (httpget != null && !httpget.isAborted()) {
                httpget.releaseConnection();
                httpget.abort();
            }

            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return responseContent;
    }

    /**
     * 通过http接口查询yarn资源使用情况
     *
     * @param url
     * @return
     */
    public static String stopYarnJob(String url) {
        String result = put(url);
        try {
            Map info = JSONUtils.parseObject(result, Map.class);
            if (info != null) {
                return String.valueOf(info.get("state"));
            }
        } catch (Exception e) {
            log.info("返回值不是json [{}]", result);
        }

        return null;
    }

    private static String put(String url) {
        String result = null;
        CloseableHttpResponse response = null;
        try {
            // 创建httpclient对象
            CloseableHttpClient httpClient = HttpClients.createDefault();

            // 创建put方式请求对象
            HttpPut httpPut = new HttpPut(url);
            httpPut.setHeader("Cache-Control", "no-store");
            httpPut.setHeader("Content-Type", "application/json");

            // 添加参数
            Map<String, String> paramMap = new HashMap<>();
            paramMap.put("state", "KILLED");
            StringEntity param = new StringEntity(JSONUtils.toPrettyJsonString((paramMap)), StandardCharsets.UTF_8);
            httpPut.setEntity(param);

            // 执行请求操作，并拿到结果（同步阻塞）
            response = httpClient.execute(httpPut);
            int code = response.getStatusLine().getStatusCode();
            // 判断网络连接状态码是否正常(0--200都数正常)
            if (code == HttpStatus.SC_OK) {
                result = EntityUtils.toString(response.getEntity(), "utf-8");
            } else if (code == HttpStatus.SC_MOVED_PERMANENTLY || code == HttpStatus.SC_MOVED_TEMPORARILY ||
                    code == HttpStatus.SC_TEMPORARY_REDIRECT) {
                Header[] locations = response.getHeaders("Location");
                if (locations != null && locations.length > 0) {
                    log.info("httpUtil put 重定向地址：{}", locations[0].getValue());
                    result = put(locations[0].getValue());
                } else {
                    log.info("httpUtil get 重定向地址为空");
                }
            } else if (code == HttpStatus.SC_ACCEPTED) {
                log.info("状态码：{} 等待500毫秒再试", code);
                Thread.sleep(500);
                result = put(url);
            } else {
                log.info("访问失败，状态码：{}", code);
            }
        } catch (Exception e) {
            log.error("HttpUtil.get error", e);
        } finally {
            if (response != null) {
                // 释放链接
                try {
                    response.close();
                } catch (IOException e) {
                    log.error("HttpUtil.get error", e);
                }
            }
        }

        return result;
    }
}
