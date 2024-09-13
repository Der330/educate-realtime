package com.atguigu.educate.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@Builder
public class DwsExaminationQuestionTestBean {
    String stt;
    String edt;
    String curDate;
    String questionId;
    Long answerCt;
    Long correctAnswerCt;
    Long answerUserCt;
    Long correctAnswerUserCt;

    @JSONField(serialize = false)
    String userId;

    @JSONField(serialize = false)
    Long ts;
}
