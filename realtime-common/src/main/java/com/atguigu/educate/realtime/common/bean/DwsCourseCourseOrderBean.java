package com.atguigu.educate.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@Builder
public class DwsCourseCourseOrderBean {
    String stt;
    String edt;
    String curDate;
    String courseId;
    String courseName;
    String subjectId;
    String subjectName;
    String categoryId;
    String categoryName;
    Long orderCt;
    BigDecimal orderAmount;
    Long courseUserCt;
    Long subjectUserCt;
    Long categoryUserCt;

    @JSONField(serialize = false)
    String userId;

    @JSONField(serialize = false)
    Long ts;
}
