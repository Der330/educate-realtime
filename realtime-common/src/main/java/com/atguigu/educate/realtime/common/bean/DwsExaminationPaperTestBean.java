package com.atguigu.educate.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@Builder
public class DwsExaminationPaperTestBean {
    String stt;
    String edt;
    String curDate;
    String paperId;
    String paperTitle;
    Long UserCt;
    BigDecimal scoreSum;
    Long durationSum;

    @JSONField(serialize = false)
    String userId;

    @JSONField(serialize = false)
    Long ts;
}
