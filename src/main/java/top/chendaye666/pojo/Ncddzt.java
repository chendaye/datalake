package top.chendaye666.pojo;

public class Ncddzt {
    private String source_type;
    private String index;
    private String agent_timestamp;
    private String topic;
    private String file_path;
    private String position;
    private String source_host;
    private String log;
    private int num;


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

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getAgent_timestamp() {
        return agent_timestamp;
    }

    public void setAgent_timestamp(String agent_timestamp) {
        this.agent_timestamp = agent_timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "Ncddzt{" +
            "source_type='" + source_type + '\'' +
            ", index='" + index + '\'' +
            ", agent_timestamp='" + agent_timestamp + '\'' +
            ", topic='" + topic + '\'' +
            ", file_path='" + file_path + '\'' +
            ", position='" + position + '\'' +
            ", source_host='" + source_host + '\'' +
            ", log='" + log + '\'' +
            ", num=" + num +
            '}';
    }
}
