package com.test.esdemo.pojo.es;

import lombok.Data;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.util.Date;
import java.util.Map;

/**
 * @author zjd
 * @Title:
 * @Description: es实体
 * @date 2021/3/1717:39
 */
@Data
public class Twitter {

    private String id;

    private String user;

    private Date postDate;

    private String message;

    private Map<String, HighlightField> highlight;


}

