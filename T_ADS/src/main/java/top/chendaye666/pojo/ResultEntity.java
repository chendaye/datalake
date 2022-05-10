package top.chendaye666.pojo;

import java.io.Serializable;

/**
 * 计算结果
 */
public class ResultEntity implements Serializable {
    private static final long serialVersionUID = 2306280590690541548L;
    String mi;
    String val_str;
    String ncddoiw2_time;
    String ncddoiw_time;
    Long diff;

    public ResultEntity() {
    }

    public ResultEntity(String mi, String val_str, String ncddoiw2_time, String ncddoiw_time, Long diff) {
        this.mi = mi;
        this.val_str = val_str;
        this.ncddoiw2_time = ncddoiw2_time;
        this.ncddoiw_time = ncddoiw_time;
        this.diff = diff;
    }

    public String getMi() {
        return mi;
    }

    public void setMi(String mi) {
        this.mi = mi;
    }

    public String getVal_str() {
        return val_str;
    }

    public void setVal_str(String val_str) {
        this.val_str = val_str;
    }

    public String getNcddoiw2_time() {
        return ncddoiw2_time;
    }

    public void setNcddoiw2_time(String ncddoiw2_time) {
        this.ncddoiw2_time = ncddoiw2_time;
    }

    public String getNcddoiw_time() {
        return ncddoiw_time;
    }

    public void setNcddoiw_time(String ncddoiw_time) {
        this.ncddoiw_time = ncddoiw_time;
    }

    public Long getDiff() {
        return diff;
    }

    public void setDiff(Long diff) {
        this.diff = diff;
    }

    @Override
    public String toString() {
        return "ResultEntity{" +
                "mi='" + mi + '\'' +
                ", val_str='" + val_str + '\'' +
                ", ncddoiw2_time='" + ncddoiw2_time + '\'' +
                ", ncddoiw_time='" + ncddoiw_time + '\'' +
                ", diff=" + diff +
                '}';
    }
}
