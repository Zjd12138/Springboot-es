package com.test.esdemo.config;

/**
 * @author zjd
 * @Title:
 * @Description: es配置类
 * @date 2021/3/1717:36
 */
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Configuration
@Component
@ConfigurationProperties(prefix = "elasticsearch")//配置注解表示这个类可以读取所有yml以 "elasticSearch"开头的配置项
public class ESConfig {

    private String host;

    private int port;

    private Integer connectNum;

    private Integer connectPerRoute;

    @Bean
    public HttpHost httpHost(){
        return new HttpHost(host,port,"http");
    }

    @Bean(initMethod="init",destroyMethod="close")
    public ESClientSpringFactory getFactory(){
        return ESClientSpringFactory.
                build(httpHost(), connectNum, connectPerRoute);
    }

    @Bean
    @Scope("singleton")
    public RestClient getRestClient(){
        return getFactory().getClient();
    }

    @Bean
    @Scope("singleton")
    public RestHighLevelClient getRHLClient(){
        return getFactory().getRhlClient();
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Integer getConnectNum() {
        return connectNum;
    }

    public void setConnectNum(Integer connectNum) {
        this.connectNum = connectNum;
    }

    public Integer getConnectPerRoute() {
        return connectPerRoute;
    }

    public void setConnectPerRoute(Integer connectPerRoute) {
        this.connectPerRoute = connectPerRoute;
    }
}

