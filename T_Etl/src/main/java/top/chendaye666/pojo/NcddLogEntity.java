package top.chendaye666.pojo;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;


public class NcddLogEntity implements Serializable {

    private static final long serialVersionUID = 3736287132798158274L;

    @JSONField(name="CHANNEL")
    private String channel = null;

    @JSONField(name="CHANNEL2")
    private String channel2 = null;

    @JSONField(name = "CHANNEL3")
    private String channel3 = null;

    @JSONField(name = "CHANNEL4")
    private String channel4 = null;

    @JSONField(name = "NODE")
    private String node = null;

    @JSONField(name = "CNT")
    private String cnt = null;

    @JSONField(name = "TIME")
    private String time = null;

    public NcddLogEntity(){

    }

    public NcddLogEntity(String channel, String channel2, String channel3, String channel4, String node, String cnt, String time) {
        this.channel = channel;
        this.channel2 = channel2;
        this.channel3 = channel3;
        this.channel4 = channel4;
        this.node = node;
        this.cnt = cnt;
        this.time = time;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getChannel2() {
        return channel2;
    }

    public void setChannel2(String channel2) {
        this.channel2 = channel2;
    }

    public String getChannel3() {
        return channel3;
    }

    public void setChannel3(String channel3) {
        this.channel3 = channel3;
    }

    public String getChannel4() {
        return channel4;
    }

    public void setChannel4(String channel4) {
        this.channel4 = channel4;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public String getCnt() {
        return cnt;
    }

    public void setCnt(String cnt) {
        this.cnt = cnt;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "NcddLogEntity{" +
                "channel='" + channel + '\'' +
                ", channel2='" + channel2 + '\'' +
                ", channel3='" + channel3 + '\'' +
                ", channel4='" + channel4 + '\'' +
                ", node='" + node + '\'' +
                ", cnt='" + cnt + '\'' +
                ", time='" + time + '\'' +
                '}';
    }
}
