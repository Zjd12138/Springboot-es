package com.test.esdemo;

import cn.hutool.core.lang.UUID;
import com.alibaba.fastjson.JSON;
import com.test.esdemo.pojo.es.Twitter;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsdemoApplicationTests {
    @Resource
    private RestHighLevelClient client;

    @Test
    public void createIndex() {
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            System.out.println(createIndexResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void indexExists(){
        GetIndexRequest request = new GetIndexRequest("twitter");
        try {
            boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
            System.out.println(exists);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteIndex() {
        DeleteIndexRequest request = new DeleteIndexRequest("goods");
        try {
            AcknowledgedResponse acknowledgedResponse =client.indices().delete(request, RequestOptions.DEFAULT);
            System.out.println(acknowledgedResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * map 方式
     */
    @Test
    public void add(){
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequest = new IndexRequest("twitter")
                .id("1").source(jsonMap);
        try {
            client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * 利用阿里巴巴 JSON库  将对象转化为 json 插入
     */
    @Test
    public void add1(){
        Twitter twitter = new Twitter();
        twitter.setId("6114155349ed4b4a966489522a5f1394");
        twitter.setMessage("hello twitter!");
        twitter.setPostDate(new Date());
        twitter.setUser("kimchy");

        IndexRequest request = new IndexRequest("twitter");
        request.id(String.valueOf(twitter.getId()));
        request.source(JSON.toJSONString(twitter), XContentType.JSON);
        try {
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 利用XContentBuilder 方式插入
     * @throws IOException
     */
    @Test
    public void add2() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("twitter")
                .id("3").source(builder);

        try {
            client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     *  UpdateRequest 第一个参数是 索引，第二个参数是 数据主键id ，表示修改id 为1 的 twitter 数据
     */
    @Test
    public void update(){
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy1");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying update Elasticsearch");
        UpdateRequest request = new UpdateRequest("twitter", "1")
                .doc(jsonMap);

        try {
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            System.out.println(updateResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    @Test
    public void update1(){
        Twitter twitter = new Twitter();
        twitter.setId("1L");
        twitter.setMessage("hello twitter update!");
        twitter.setPostDate(new Date());
        twitter.setUser("kimchy");
        UpdateRequest request = new UpdateRequest("twitter", String.valueOf(twitter.getId()))
                .doc(JSON.toJSONString(twitter), XContentType.JSON);

        try {
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            System.out.println(updateResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void update2() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying update Elasticsearch");
        }
        builder.endObject();
        UpdateRequest request = new UpdateRequest("twitter", "1")
                .doc(builder);
        try {
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            System.out.println(updateResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除id 为1 的数据
     */
    @Test
    public void deleteById() {

        DeleteRequest request = new DeleteRequest("twitter", "4");
        try {
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

            System.out.println(deleteResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 获取id 为1 的数据
     */
    @Test
    public void getById(){
        GetRequest getRequest = new GetRequest("twitter", "1");
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                Map<String,Object> map = getResponse.getSourceAsMap();
                Twitter twitter=JSON.parseObject(JSON.toJSONString(map), Twitter.class);
                System.out.println(twitter);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * QueryBuilders.termQuery(“key”, “vaule”); // 完全匹配
     * QueryBuilders.termsQuery(“key”, “vaule1”, “vaule2”) ; //一次匹配多个值
     * QueryBuilders.matchQuery(“key”, “vaule”) //单个匹配, field不支持通配符, 前缀具高级特性
     * QueryBuilders.multiMatchQuery(“text”, “field1”, “field2”); //匹配多个字段, field有通配符忒行
     * QueryBuilders.matchAllQuery(); // 匹配所有文件
     * 组合查询
     * // Bool Query 用于组合多个叶子或复合查询子句的默认查询
     // must 相当于 与 & =
     // must not 相当于 非 ~ ！=
     // should 相当于 或 | or
     // filter 过滤
     QueryBuilders.boolQuery()
     .must(QueryBuilders.termQuery(“key”, “value1”))
     .must(QueryBuilders.termQuery(“key”, “value2”))
     .mustNot(QueryBuilders.termQuery(“key”, “value3”))
     .should(QueryBuilders.termQuery(“key”, “value4”))
     .filter(QueryBuilders.termQuery(“key”, “value5”));
     * */

    @Test
    public void search(){
        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //默认分词查询
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("user", "你好企业")
                .fuzziness(Fuzziness.AUTO) //模糊查询
                .prefixLength(3) // 在匹配查询上设置前缀长度选项,指明区分词项的共同前缀长度，默认是0
                .maxExpansions(10); //设置最大扩展选项以控制查询的模糊过程

        //查询条件 添加，user = kimchy
        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        //sourceBuilder.query(matchQueryBuilder);

        //查询开始-结束 。可以用来分页使用
        sourceBuilder.from(0);
        sourceBuilder.size(5);

        //设置一个可选的超时，控制允许搜索的时间。
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //排序
        sourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC));
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            //处理搜索结果
            RestStatus restStatus = searchResponse.status();
            if (restStatus != RestStatus.OK){
                System.out.println("搜索错误");
            }
            List<Twitter> list = new ArrayList<>();
            SearchHits hits = searchResponse.getHits();
            hits.forEach(item -> list.add(JSON.parseObject(item.getSourceAsString(), Twitter.class)));
            System.out.println(list);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 高亮搜索
     * @param indexName 索引名称
     * @param queryBuilder 查询条件
     * @param highligtFiled 高亮字段
     * @return
     */
    public SearchResponse searcherHighlight(String indexName,QueryBuilder queryBuilder, String highligtFiled) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//构造搜索对象
        searchSourceBuilder.query(queryBuilder);//设置查询条件
        //设置高亮
        String preTags = "<strong>";
        String postTags = "</strong>";
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags(preTags);//设置前缀
        highlightBuilder.postTags(postTags);//设置后缀
        highlightBuilder.field(highligtFiled);//设置高亮字段
        searchSourceBuilder.highlighter(highlightBuilder);//设置高亮信息

        SearchRequest searchRequest = new SearchRequest(indexName);//创建查询请求对象
        searchRequest.source(searchSourceBuilder);//设置searchSourceBuilder

        SearchResponse searchResponse = null;//执行查询
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return searchResponse;
    }

    @Test
    public void highlighting(){
        String indexName = "twitter";//索引名称
        String highligtFiled = "message";//设置高亮的字段，此处查询的是interest中含有basketball的文档，因此高亮字段设为interest
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("message",
                "hello");//查询message中含有Elasticsearch的文档
        SearchResponse searchResponse = searcherHighlight(indexName,
                queryBuilder, highligtFiled);

        //处理搜索结果
        RestStatus restStatus = searchResponse.status();
        if (restStatus != RestStatus.OK){
            System.out.println("搜索错误");
        }
        List<Twitter> list = new ArrayList<>();
        SearchHits hits = searchResponse.getHits();
        hits.forEach(item -> {
                    Twitter twitter = JSON.parseObject(item.getSourceAsString(), Twitter.class);
                    Map<String, HighlightField> map = item.getHighlightFields()  ;
                    System.out.println(map.toString());
                     Set<Map.Entry<String, HighlightField>> entries = map.entrySet();
                    twitter.setHighlight(map);
                    list.add(twitter);
                }
        );

        for ( Twitter s:list) {
            System.out.println(s.getId());
        }

    }




}
