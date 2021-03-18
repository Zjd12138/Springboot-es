package com.test.esdemo.pojo.es;

import lombok.Data;

import java.util.Date;

/**
 * @author zjd
 * @Title:
 * @Description:
 * @date 2021/3/189:46
 */
@Data
public class ClassIndex extends  BaesES{
    private String id;

    private Date creatTime;

    private Date updateTime;

    private String name;

    private String message;

    private Integer age;

}
