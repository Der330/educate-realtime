package com.atguigu.educate.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@Builder
public class DwsExaminationPaperScoreLevelTestBean {
    String stt;
    String edt;
    String curDate;
    String paperId;
    String paperTitle;
    String scoreLevel;
    Long UserCt;

    @JSONField(serialize = false)
    String userId;

    @JSONField(serialize = false)
    Long ts;
}
