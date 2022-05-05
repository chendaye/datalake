package top.chendaye666.pojo;

import java.io.Serializable;
import java.sql.Timestamp;

public class CommonTableEntity implements Serializable {

    private static final long serialVersionUID = -4727606735952323159L;
    String table_name;
    String source_type;
    String mi;
    String time;
    String date;
    long created_at;
    String node;
    String channel;
    String channel2;
    String channel3;
    String channel4;
    String channel5;
    String channel6;
    float val;
    String val_str;
//    Timestamp rowtime;

    public CommonTableEntity(){}

    public CommonTableEntity(String table_name, String source_type, String mi, String time, String date, long created_at, String node, String channel, String channel2, String channel3, String channel4, String channel5, String channel6, float val, String val_str) {
        this.table_name = table_name;
        this.source_type = source_type;
        this.mi = mi;
        this.time = time;
        this.date = date;
        this.created_at = created_at;
        this.node = node;
        this.channel = channel;
        this.channel2 = channel2;
        this.channel3 = channel3;
        this.channel4 = channel4;
        this.channel5 = channel5;
        this.channel6 = channel6;
        this.val = val;
        this.val_str = val_str;
    }

    public String getMi() {
        return mi;
    }

    public void setMi(String mi) {
        this.mi = mi;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
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

    public String getChannel5() {
        return channel5;
    }

    public void setChannel5(String channel5) {
        this.channel5 = channel5;
    }

    public String getChannel6() {
        return channel6;
    }

    public void setChannel6(String channel6) {
        this.channel6 = channel6;
    }

    public float getVal() {
        return val;
    }

    public void setVal(float val) {
        this.val = val;
    }

    public String getVal_str() {
        return val_str;
    }

    public void setVal_str(String val_str) {
        this.val_str = val_str;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getSource_type() {
        return source_type;
    }

    public void setSource_type(String source_type) {
        this.source_type = source_type;
    }

    public long getCreated_at() {
        return created_at;
    }

    public void setCreated_at(long created_at) {
        this.created_at = created_at;
    }


    @Override
    public String toString() {
        return "CommonTableEntity{" +
                "table_name='" + table_name + '\'' +
                ", source_type='" + source_type + '\'' +
                ", mi='" + mi + '\'' +
                ", time='" + time + '\'' +
                ", date='" + date + '\'' +
                ", created_at=" + created_at +
                ", node='" + node + '\'' +
                ", channel='" + channel + '\'' +
                ", channel2='" + channel2 + '\'' +
                ", channel3='" + channel3 + '\'' +
                ", channel4='" + channel4 + '\'' +
                ", channel5='" + channel5 + '\'' +
                ", channel6='" + channel6 + '\'' +
                ", val=" + val +
                ", val_str='" + val_str + '\'' +
                '}';
    }
}
