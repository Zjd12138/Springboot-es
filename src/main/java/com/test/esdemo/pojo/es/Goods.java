package com.test.esdemo.pojo.es;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author zjd
 * @Title:
 * @Description: 测试
 * @date 2021/3/1810:09
 */
@Data
public class Goods extends BaesES {

    private String title;

    private String brandName;

    private BigDecimal price;
}
