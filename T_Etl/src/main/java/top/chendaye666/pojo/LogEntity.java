package top.chendaye666.pojo;


import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;

public class LogEntity implements Serializable {

    private static final long serialVersionUID = 3351000388098742925L;

    @JSONField(name="aGENT_TIMESTAMP")
    public String agent_timestamp;

    @JSONField(name="fILE_PATH")
    public String file_path;

    @JSONField(name="iNDEX")
    public String index;

    @JSONField(name="lOG")
    public String log;

    @JSONField(name="pOSITION")
    public String position;

    @JSONField(name="sOURCE_HOST")
    public String source_host;

    @JSONField(name="sOURCE_TYPE")
    public String source_type;

    @JSONField(name="tOPIC")
    public String topic;

    public LogEntity() {
    }

    public LogEntity(String agent_timestamp, String file_path, String index, String log, String position, String source_host, String source_type, String topic) {
        this.agent_timestamp = agent_timestamp;
        this.file_path = file_path;
        this.index = index;
        this.log = log;
        this.position = position;
        this.source_host = source_host;
        this.source_type = source_type;
        this.topic = topic;
    }

    public String getAgent_timestamp() {
        return agent_timestamp;
    }

    public void setAgent_timestamp(String agent_timestamp) {
        this.agent_timestamp = agent_timestamp;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getSource_host() {
        return source_host;
    }

    public void setSource_host(String source_host) {
        this.source_host = source_host;
    }

    public String getSource_type() {
        return source_type;
    }

    public void setSource_type(String source_type) {
        this.source_type = source_type;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "LogEntity{" +
                "agent_timestamp='" + agent_timestamp + '\'' +
                ", file_path='" + file_path + '\'' +
                ", index='" + index + '\'' +
                ", log='" + log + '\'' +
                ", position='" + position + '\'' +
                ", source_host='" + source_host + '\'' +
                ", source_type='" + source_type + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
