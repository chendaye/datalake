package top.chendaye666.pojo;

public class NcddztDws {
  private String source_type;
  private String index;
  private String agent_timestamp;
  private String topic;
  private long time;
  private long sum;

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

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public long getSum() {
    return sum;
  }

  public void setSum(long sum) {
    this.sum = sum;
  }

  @Override
  public String toString() {
    return "NcddztDws{" +
        "source_type='" + source_type + '\'' +
        ", index='" + index + '\'' +
        ", agent_timestamp='" + agent_timestamp + '\'' +
        ", topic='" + topic + '\'' +
        ", time=" + time +
        ", sum=" + sum +
        '}';
  }
}
