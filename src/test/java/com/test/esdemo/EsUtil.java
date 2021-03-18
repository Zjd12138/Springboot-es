package com.test.esdemo;

import com.test.esdemo.config.ElasticsearchUtil;
import com.test.esdemo.pojo.es.Twitter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Date;

/**
 * @author zjd
 * @Title:
 * @Description: 测试1
 * @date 2021/3/1718:43
 */
@SpringBootTest
public class EsUtil {

    @Autowired
    private ElasticsearchUtil elasticsearchUtil;

    @Test
    public void add() throws IOException {
        Twitter twitter = new Twitter();
        twitter.setMessage("hello twitter!");
        twitter.setPostDate(new Date());
        twitter.setUser("kimchy");
        String twitter1 = elasticsearchUtil.addData(twitter, "twitter");
        System.out.println(twitter1);
    }

}
