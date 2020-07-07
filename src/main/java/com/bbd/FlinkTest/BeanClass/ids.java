package com.bbd.FlinkTest.BeanClass;

/**
 * Copyright@paidaxing
 * Author: paidaxing
 * Date:2020/7/3
 * Description:
 */
public class ids {
    public static String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public ids(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "ids{" +
                "id='" + id + '\'' +
                '}';
    }
}
