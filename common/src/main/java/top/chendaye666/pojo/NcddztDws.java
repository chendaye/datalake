package top.chendaye666.pojo;

import java.io.Serializable;

public class NcddztDws implements Serializable {
  private static final long serialVersionUID = -6443126064314892424L;

  private String source_type;
  private String agent_timestamp;
  private String topic;
  private Long total;

  public NcddztDws() {
  }

  public NcddztDws(String source_type, String agent_timestamp, String topic, Long total) {
    this.source_type = source_type;
    this.agent_timestamp = agent_timestamp;
    this.topic = topic;
    this.total = total;
  }

  public Long getTotal() {
    return total;
  }

  public void setTotal(Long total) {
    this.total = total;
  }

  public String getSource_type() {
    return source_type;
  }

  public void setSource_type(String source_type) {
    this.source_type = source_type;
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



  @Override
  public String toString() {
    return "NcddztDws{" +
        "source_type='" + source_type + '\'' +
        ", agent_timestamp='" + agent_timestamp + '\'' +
        ", topic='" + topic + '\'' +
        ", total=" + total +
        '}';
  }
}
