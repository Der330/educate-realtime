package com.atguigu.eaducat.realtime.dws.app;

import com.atguigu.educate.realtime.common.bean.DwsUserPageBean;

public class main1 {
    public static void main(String[] args) {

        DwsUserPageBean userPageBean=DwsUserPageBean.builder().build();
        userPageBean.initBean();
        System.out.println(userPageBean.getAddCarCount());
    }
}
