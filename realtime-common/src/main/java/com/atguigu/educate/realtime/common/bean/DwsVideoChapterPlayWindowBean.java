package com.atguigu.educate.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class DwsVideoChapterPlayWindowBean {
    String stt;
    String edt;
    String curDate;
    String videoId;
    String chapterId;
    String chapterName;
    Long playCt;
    Long playDuration;
    Long playUserCt;

    @JSONField(serialize = false)
    String mid;

    @JSONField(serialize = false)
    Long ts;
}
