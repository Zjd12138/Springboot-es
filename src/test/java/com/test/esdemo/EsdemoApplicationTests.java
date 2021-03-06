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
     * map ??????
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
     * ?????????????????? JSON???  ?????????????????? json ??????
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
     * ??????XContentBuilder ????????????
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
     *  UpdateRequest ?????????????????? ??????????????????????????? ????????????id ???????????????id ???1 ??? twitter ??????
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
     * ??????id ???1 ?????????
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
     * ??????id ???1 ?????????
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
     * QueryBuilders.termQuery(???key???, ???vaule???); // ????????????
     * QueryBuilders.termsQuery(???key???, ???vaule1???, ???vaule2???) ; //?????????????????????
     * QueryBuilders.matchQuery(???key???, ???vaule???) //????????????, field??????????????????, ?????????????????????
     * QueryBuilders.multiMatchQuery(???text???, ???field1???, ???field2???); //??????????????????, field??????????????????
     * QueryBuilders.matchAllQuery(); // ??????????????????
     * ????????????
     * // Bool Query ????????????????????????????????????????????????????????????
     // must ????????? ??? & =
     // must not ????????? ??? ~ ???=
     // should ????????? ??? | or
     // filter ??????
     QueryBuilders.boolQuery()
     .must(QueryBuilders.termQuery(???key???, ???value1???))
     .must(QueryBuilders.termQuery(???key???, ???value2???))
     .mustNot(QueryBuilders.termQuery(???key???, ???value3???))
     .should(QueryBuilders.termQuery(???key???, ???value4???))
     .filter(QueryBuilders.termQuery(???key???, ???value5???));
     * */

    @Test
    public void search(){
        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //??????????????????
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("user", "????????????")
                .fuzziness(Fuzziness.AUTO) //????????????
                .prefixLength(3) // ??????????????????????????????????????????,???????????????????????????????????????????????????0
                .maxExpansions(10); //??????????????????????????????????????????????????????

        //???????????? ?????????user = kimchy
        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        //sourceBuilder.query(matchQueryBuilder);

        //????????????-?????? ???????????????????????????
        sourceBuilder.from(0);
        sourceBuilder.size(5);

        //????????????????????????????????????????????????????????????
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //??????
        sourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC));
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            //??????????????????
            RestStatus restStatus = searchResponse.status();
            if (restStatus != RestStatus.OK){
                System.out.println("????????????");
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
     * ????????????
     * @param indexName ????????????
     * @param queryBuilder ????????????
     * @param highligtFiled ????????????
     * @return
     */
    public SearchResponse searcherHighlight(String indexName,QueryBuilder queryBuilder, String highligtFiled) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//??????????????????
        searchSourceBuilder.query(queryBuilder);//??????????????????
        //????????????
        String preTags = "<strong>";
        String postTags = "</strong>";
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags(preTags);//????????????
        highlightBuilder.postTags(postTags);//????????????
        highlightBuilder.field(highligtFiled);//??????????????????
        searchSourceBuilder.highlighter(highlightBuilder);//??????????????????

        SearchRequest searchRequest = new SearchRequest(indexName);//????????????????????????
        searchRequest.source(searchSourceBuilder);//??????searchSourceBuilder

        SearchResponse searchResponse = null;//????????????
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return searchResponse;
    }

    @Test
    public void highlighting(){
        String indexName = "twitter";//????????????
        String highligtFiled = "message";//??????????????????????????????????????????interest?????????basketball????????????????????????????????????interest
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("message",
                "hello");//??????message?????????Elasticsearch?????????
        SearchResponse searchResponse = searcherHighlight(indexName,
                queryBuilder, highligtFiled);

        //??????????????????
        RestStatus restStatus = searchResponse.status();
        if (restStatus != RestStatus.OK){
            System.out.println("????????????");
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
