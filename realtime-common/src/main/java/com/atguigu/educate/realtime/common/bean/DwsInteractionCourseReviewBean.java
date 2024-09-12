package com.atguigu.educate.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class DwsInteractionCourseReviewBean {
    String stt;
    String edt;
    String curDate;
    String courseId;
    String courseName;
    Long reviewUserCt;
    Long goodReviewUserCt;
    Long reviewScoreSum;
    @JSONField(serialize = false)
    Long ts;
}
