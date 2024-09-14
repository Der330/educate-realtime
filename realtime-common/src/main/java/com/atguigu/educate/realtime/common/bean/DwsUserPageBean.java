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
    String curDate;
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

    @JSONField(serialize = false)
    public DwsUserPageBean add(DwsUserPageBean value){

        this.setHomeViewCount(this.getHomeViewCount()+ value.getHomeViewCount());
        this.setDetailViewCount(this.getDetailViewCount()+value.getDetailViewCount());
        this.setAddCarCount(this.getAddCarCount()+ value.getAddCarCount());
        this.setDoOrderCount(this.getDoOrderCount()+value.getDoOrderCount());
        this.setPaySucCount(this.getPaySucCount()+ value.getPaySucCount());

        return this;
    }
}
