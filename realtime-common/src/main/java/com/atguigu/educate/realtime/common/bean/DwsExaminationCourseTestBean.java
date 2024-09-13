package com.atguigu.educate.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@Builder
public class DwsExaminationCourseTestBean {
    String stt;
    String edt;
    String curDate;
    String courseId;
    String courseName;
    Long UserCt;
    BigDecimal scoreSum;
    Long durationSum;

    @JSONField(serialize = false)
    String userId;

    @JSONField(serialize = false)
    String paperId;

    @JSONField(serialize = false)
    Long ts;
}
