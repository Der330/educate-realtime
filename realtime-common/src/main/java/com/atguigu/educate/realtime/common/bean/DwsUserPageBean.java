package com.atguigu.educate.realtime.common.bean;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class DwsUserPageBean {
    String stt;
    String edt;
    String currDate;
    Long homeViewCount;
    Long detailViewCount;
    Long addCarCount;
    Long doOrderCount;
    Long paySucCount;

    @JSONField(serialize = false)
    Long ts;

    @JSONField(serialize = false)
    public void initBean(){
        this.setHomeViewCount(0L);
        this.setDetailViewCount(0L);
        this.setAddCarCount(0L);
        this.setDoOrderCount(0L);
        this.setPaySucCount(0L);
    }
}
