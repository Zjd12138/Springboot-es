package com.test.esdemo.pojo.es;

import lombok.Data;

import java.util.Date;

/**
 * @author zjd
 * @Title:
 * @Description: 父类
 * @date 2021/3/189:44
 */
@Data
public class BaesES {
    private String id;

    private Date creatTime;

    private Date updateTime;

}
