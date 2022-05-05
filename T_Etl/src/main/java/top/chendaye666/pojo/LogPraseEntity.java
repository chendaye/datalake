package top.chendaye666.pojo;

import java.io.Serializable;

public class LogPraseEntity implements Serializable {
    private static final long serialVersionUID = -503428419026895387L;

    private String date;
    private String log;

    public LogPraseEntity(){}

    public LogPraseEntity(String date, String log) {
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
        return "LogPraseEntity{" +
                "date='" + date + '\'' +
                ", log='" + log + '\'' +
                '}';
    }
}
