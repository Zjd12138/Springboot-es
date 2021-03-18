package com.test.esdemo;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import com.test.esdemo.pojo.es.BaesES;
import com.test.esdemo.pojo.es.ClassIndex;
import com.test.esdemo.pojo.es.Goods;


import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author zjd
 * @Title:
 * @Description:
 * @date 2021/3/189:47
 */
@SpringBootTest
class ClassinfoTest {
    @Resource
    private RestHighLevelClient client;

    @Test
    public void createIndex() {
        /**
         * 索引必须全小写
         * */
        CreateIndexRequest request = new CreateIndexRequest("classindex");
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            System.out.println(createIndexResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void indexExists(){
        GetIndexRequest request = new GetIndexRequest("classindex");
        try {
            boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
            System.out.println(exists);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建映射
     * */
    @Test
    public void addIndexAndMapping() throws IOException {
        //1.使用client获取操作索引的对象
        IndicesClient indicesClient = client.indices();
        //2.具体操作，获取返回值
        CreateIndexRequest createRequest = new CreateIndexRequest("itcast");
        //2.1 设置mappings
        String mapping = "{\n" +
                "\t\"properties\": {\n" +
                "\t\t\"title\": {\n" +
                "\t\t\t\"type\": \"text\",\n" +
                "\t\t\t\"analyzer\": \"ik_smart\"\n" +
                "\t\t},\n" +
                "\t\t\"price\": { \n" +
                "\t\t\t\"type\": \"double\"\n" +
                "\t\t},\n" +
                "\t\t\"createTime\": {\n" +
                "\t\t\t\"type\": \"date\"\n" +
                "\t\t},\n" +
                "\t\t\"categoryName\": {\t\n" +
                "\t\t\t\"type\": \"keyword\"\n" +
                "\t\t},\n" +
                "\t\t\"brandName\": {\t\n" +
                "\t\t\t\"type\": \"keyword\"\n" +
                "\t\t},\n" +
                "\n" +
                "\t\t\"spec\": {\t\t\n" +
                "\t\t\t\"type\": \"object\"\n" +
                "\t\t},\n" +
                "\t\t\"saleNum\": {\t\n" +
                "\t\t\t\"type\": \"integer\"\n" +
                "\t\t},\n" +
                "\t\t\n" +
                "\t\t\"stock\": {\t\n" +
                "\t\t\t\"type\": \"integer\"\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";
        createRequest.mapping(mapping,XContentType.JSON);
        CreateIndexResponse response = indicesClient.create(createRequest, RequestOptions.DEFAULT);

        //3.根据返回值判断结果
        System.out.println(response.isAcknowledged());
    }

    @Test
    public void add() throws IOException {
        ClassIndex twitter = new ClassIndex();
        twitter.setMessage("hello twitter!");
        twitter.setAge(10);
        twitter.setName("kimchy");
        String twitter1 = addData(twitter, "classindex");
        System.out.println(twitter1);
    }

    @Test
    public void getById(){
        GetRequest getRequest = new GetRequest("classindex", "DBA9ED9F82E5467C9AF499E766BE3673");
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                Map<String,Object> map = getResponse.getSourceAsMap();
                ClassIndex classIndex=JSON.parseObject(JSON.toJSONString(map), ClassIndex.class);
                System.out.println(classIndex);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void importData() throws IOException {
        //1.查询所有数据，mysql
       // List<Goods> goodsList = goodsMapper.findAll();
        //System.out.println(goodsList.size());

        List<Goods> goodsList=getList();

        //2.bulk导入
        BulkRequest bulkRequest = new BulkRequest();
        //2.1 循环goodsList，创建IndexRequest添加数据
        for (Goods good : goodsList) {
            //2.2 设置spec规格信息 Map的数据   specStr:{}
            //goods.setSpec(JSON.parseObject(goods.getSpecStr(),Map.class));
            /*String specStr = goods.getSpecStr();
            //将json格式字符串转为Map集合
            Map map = JSON.parseObject(specStr, Map.class);
            //设置spec map
            goods.setSpec(map);*/
            //将goods对象转换为json字符串
            String data = JSON.toJSONString(good);//map --> {}
            IndexRequest indexRequest = new IndexRequest("goods");
            indexRequest.id(good.getId()).source(data, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    public List<Goods> getList(){
        List<Goods> goodsList=new ArrayList<>();
        for (int i=0 ;i<10; i++ ) {
            Goods goods = new Goods();
            goods.setId(i+"");
            goods.setTitle("华为");
            int c=i*100;
            goods.setPrice(new BigDecimal(c));
            goods.setBrandName("华为"+i);
            goodsList.add(goods);
        }


        return goodsList;
    }


    @Test
    public void find(){
        GetRequest getRequest = new GetRequest("classindex", "DBA9ED9F82E5467C9AF499E766BE3673");
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                Map<String,Object> map = getResponse.getSourceAsMap();
                ClassIndex classIndex=JSON.parseObject(JSON.toJSONString(map), ClassIndex.class);
                System.out.println(classIndex);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public   String addData(BaesES baesES, String index, String id) throws IOException {
        //创建请求
        baesES.setId(id);
        baesES.setCreatTime(new Date());
        IndexRequest request = new IndexRequest(index);
        //规则 put /test_index/_doc/1 将数据放入请求 json
        request.id(id).source(JSON.toJSONString(baesES), XContentType.JSON);;
        //request.timeout(TimeValue.timeValueSeconds(1));
        //客户端发送请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        return response.getId();
    }

    /**
     * 数据添加 随机id
     * @param baesES 要增加的数据
     * @param index      索引，类似数据库
     * @return
     */
    public  String addData(BaesES baesES, String index) throws IOException {
        return addData(baesES, index, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
    }

    @Test
    public void search(Map searchMap) throws IOException {
        //1. 构建查询请求对象，指定查询的索引名称
        SearchRequest searchRequest=new  SearchRequest("goods");
        //2. 创建查询条件构建器SearchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //多条件拼接，布尔查询构建器
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //3. 封装查询条件、查询方式
        //3.1 基础查询（使用QueryBuilders构建的查询对象），使用boolQueryBuilder封装
        if(StringUtils.isNotBlank((String) searchMap.get("keyword"))){
            QueryBuilder keyword = QueryBuilders.matchQuery("title", searchMap.get("keyword"));
            boolQueryBuilder.filter(keyword);
        }

        //3.2 高级查询（分页，排序、分组、高亮）
       /* if(分页判断){
            searchSourceBuilder.from(curPage);
            searchSourceBuilder.size(size);
        }
        if(分组聚合判断){//可以有多个；Aggregation：聚合
            AggregationBuilder agg1 = AggregationBuilders.terms("goods_brands").field("brandName").size(100);
            AggregationBuilder agg2 = AggregationBuilders.terms("goods_category").field("categoryName").size(100);
        ...
            searchSourceBuilder.aggregation(agg1);
            searchSourceBuilder.aggregation(agg2);
        }*/
        //高亮判断
        if(StringUtils.isNotBlank((String) searchMap.get("keyword"))){
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.field("title");
            highlightBuilder.preTags("");
            highlightBuilder.postTags("");
            searchSourceBuilder.highlighter(highlightBuilder);
        }
        //4. 整合所有查询条件
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        //5. 使用RestHighLevelClient执行搜索searchRequest
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //6. 获取搜索结果
        SearchHits searchHits = searchResponse.getHits();

        //6.1 获取总记录数
        long value = searchHits.getTotalHits().value;
        System.out.println("总记录数："+value);
        List<Goods> goodsList = new ArrayList<>();
        //6.2 数据列表
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            //获取json字符串格式的数据
            String sourceAsString = hit.getSourceAsString();
            //转为java对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);
            //6.3 高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField HighlightField = highlightFields.get("title");
            Text[] fragments = HighlightField.fragments();
            //替换为“高亮内容”
            if(fragments != null && fragments.length > 0){
                goods.setTitle(fragments[0].toString());
            }
            goodsList.add(goods);
        }

        //6.4 分组结果
        // 获取聚合结果
        List brands = new ArrayList();
        Aggregations aggregations = searchResponse.getAggregations();
        Map<String, Aggregation> aggregationMap = aggregations.asMap();

        Terms goods_brands = (Terms) aggregationMap.get("goods_brands");
        List<? extends Terms.Bucket> buckets = goods_brands.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            Object key = bucket.getKey();
            brands.add(key);
        }
    }

    /**
     *QueryBuilders构建：
     *     1）match_all 查询所有
     *         QueryBuilder query = QueryBuilders.matchAllQuery();//查询所有文档
           2）term 词条查询：不分词，精确匹配
               QueryBuilder query = QueryBuilders.termQuery("title","华为");
           3）match 分词查询：先分词，后查询（分词后默认使用"or"连接条件）
               QueryBuilder query = QueryBuilders.macthQuery("title", "华为手机");
          4) wildcard 模糊查询
            WildcardQueryBuilder query = QueryBuilders.wildcardQuery("title", "华*");
          5) regexp 正则查询
            RegexpQueryBuilder query = QueryBuilders.regexpQuery("title", "\\w+(.)*");
         6) prefix 前缀查询
            PrefixQueryBuilder query = QueryBuilders.prefixQuery("brandName", "三");
         7) range 范围查询
             1)QueryBuilder query = QueryBuilders.rangeQuery("").gte("").lte("");
             //范围查询
             2)RangeQueryBuilder query = QueryBuilders.rangeQuery("price");
             //指定下限
             query.gte(2000);
             //指定上限
             query.lte(3000);
     8) queryString 条件字符串查询（作用：一值多域(输入华为,在title和category等等有,都可查)查询，支持分词）
          QueryStringQueryBuilder query = QueryBuilders.queryStringQuery("华为手机").field("title").
                                 field("categoryName").field("brandName").defaultOperator(Operator.AND);
            queryStringQuery支持and和or的连接符；
            simplequeryStringQuery不支持
     9) bool 布尔查询，多条件拼接的条件构建器.连接的方式有下面4种-----》对8）多条件查询的拼接
     must：(and)必须满足，计算分值
     must_not：(not)必须不满足，计算分值
     should：(or)可以满足，计算分值
     ---------------------------------
     filter：必须满足，不计算分值（效率高）

     BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

     *
     * */

    /**
     * 9,布尔查询：boolQuery
     *  1. 查询品牌名称为:华为
     *  2. 查询标题包含：手机
     *  3. 查询价格在：2000-3000
     */
    @Test
    public void testBoolQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBulider = new SearchSourceBuilder();
        //1.构建boolQuery
        BoolQueryBuilder query = QueryBuilders.boolQuery();

        //2.构建各个查询条件
        //2.1 查询品牌名称为:华为
        QueryBuilder termQuery = QueryBuilders.termQuery("brandName","华为");
        query.must(termQuery);

        //2.2. 查询标题包含：手机
        QueryBuilder matchQuery = QueryBuilders.matchQuery("title","手机");
        query.filter(matchQuery);

        //2.3 查询价格在：2000-3000;gte大于等于;lte小于等于
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price");
        rangeQuery.gte(2000);
        rangeQuery.lte(3000);
        query.filter(rangeQuery);

        //3.使用boolQuery连接
        sourceBulider.query(query);
        searchRequest.source(sourceBulider);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = searchResponse.getHits();

        //获取记录数
        long value = searchHits.getTotalHits().value;
        System.out.println("总记录数："+value);

        List<Goods> goodsList = new ArrayList<>();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            //转为java
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);
            goodsList.add(goods);
        }

        for (Goods goods : goodsList) {
            System.out.println(goods);
        }

    }

    /**
     * SearchSourceBuilder
     *  1）分页
     * 		searchSourceBuilder.from(0)
     *         searchSourceBuilder.size(10)
     * 	2）排序
     * 		searchSourceBuilder.sort(“field”,SortOrder.DESC|SortOrder.ASC);
     *  3）高亮（指定高亮域、设置高亮前缀、后缀）
     * 	4）分组聚合
     * 		(指标聚合(max,min,avg,sum)和桶聚合(groupby),桶聚合不能对text类型的使用:例:搜索面板上的品牌)
     * 	    //goods_brands是自己起的名字,brandName桶聚合的字段
     * 		  AggregationBuilder agg = AggregationBuilders.terms("goods_brands").field("brandName").size(100);
     * 		   sourceBulider.aggregation(agg);
     * */

    /**
     *
     * 11,高亮查询：
     *      1. 设置高亮
     *          * 高亮字段
     *          * 前缀
     *          * 后缀
     *      2. 将高亮了的字段数据，替换原有数据
     */

    @Test
    public void testHighLightQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBulider = new SearchSourceBuilder();

        // 1. 查询title包含手机的数据
        MatchQueryBuilder query = QueryBuilders.matchQuery("title", "华为");
        //设置高亮
        HighlightBuilder highlighter = new HighlightBuilder();
        //设置三要素(前后缀有默认的<em></em>)
        highlighter.field("title");
        highlighter.preTags("<font color='red'>");
        highlighter.postTags("</font>");
        sourceBulider.query(query);
        sourceBulider.highlighter(highlighter);
        // 2. 查询品牌列表
        /*
        参数：
            1. 自定义的名称，将来用于获取数据
            2. 分组的字段
         */
        AggregationBuilder agg = AggregationBuilders.terms("goods_price").field("price").size(100);
        AggregationBuilder subagg = AggregationBuilders.terms("goods_price").field("price").size(100);
        agg.subAggregation(subagg);
        sourceBulider.aggregation(agg);
        searchRequest.source(sourceBulider);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = searchResponse.getHits();
        //获取记录数
        long value = searchHits.getTotalHits().value;
        System.out.println("总记录数："+value);

        List<Goods> goodsList = new ArrayList<>();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            //转为java
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);
            // 获取高亮结果，替换goods中的title
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField HighlightField = highlightFields.get("title");
            //高亮片段(现在只有一个:是数组的第一个元素);域可能是多值得,所以返回的数组
            Text[] fragments = HighlightField.fragments();
            //替换
            goods.setTitle(fragments[0].toString());
            goodsList.add(goods);
        }

        for (Goods goods : goodsList) {
            System.out.println(goods);
        }

        // 获取聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        if(ObjectUtil.isNotEmpty(aggregations)){
            Map<String, Aggregation> aggregationMap = aggregations.asMap();
            //System.out.println(aggregationMap);
            Terms goods_brands = (Terms) aggregationMap.get("goods_price");
            List<? extends Terms.Bucket> buckets = goods_brands.getBuckets();
            List brands = new ArrayList();
            for (Terms.Bucket bucket : buckets) {
                Object key = bucket.getKey();
                brands.add(key);
            }

            for (Object brand : brands) {
                System.out.println(brand);
            }
        }

    }
    /**
     * -------QueryBuilders.-----------
     * 1）term
     * 2）match
     * 3）range
     * 4）bool
     * -------SearchSourceBuilder-----------
     * 5）page
     * 6）sort
     * 7）highlight
     * 8）aggregation
     *
     * */
}