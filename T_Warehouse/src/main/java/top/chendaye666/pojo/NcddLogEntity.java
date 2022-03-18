package top.chendaye666.pojo;

import java.io.Serializable;

public class NcddLogEntity implements Serializable {
    private static final long serialVersionUID = 3463413310197795859L;

    String date;
    String log;
    public NcddLogEntity(){

    }

    public NcddLogEntity(String date, String log) {
        this.date = date;
        this.log = log;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    @Override
    public String toString() {
        return "NcddLogEntity{" +
                "date='" + date + '\'' +
                ", log='" + log + '\'' +
                '}';
    }
}
