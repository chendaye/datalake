package top.chendaye666.pojo;

public class NcddztDwd {
  private String source_type;
  private String index;
  private String agent_timestamp;
  private String source_host;
  private String topic;
  private String file_path;
  private String position;
  private long time;
  private String log_type;
  private String qd_number;
  private String seat;
  private String market;
  private String cap_acc;
  private String suborderno;
  private String wt_pnum;
  private String contract_num;

  public NcddztDwd() {
  }

  public NcddztDwd(String source_type, String index, String agent_timestamp, String source_host, String topic, String file_path, String position, long time, String log_type, String qd_number, String seat, String market, String cap_acc, String suborderno, String wt_pnum, String contract_num) {
    this.source_type = source_type;
    this.index = index;
    this.agent_timestamp = agent_timestamp;
    this.source_host = source_host;
    this.topic = topic;
    this.file_path = file_path;
    this.position = position;
    this.time = time;
    this.log_type = log_type;
    this.qd_number = qd_number;
    this.seat = seat;
    this.market = market;
    this.cap_acc = cap_acc;
    this.suborderno = suborderno;
    this.wt_pnum = wt_pnum;
    this.contract_num = contract_num;
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

  public String getSource_host() {
    return source_host;
  }

  public void setSource_host(String source_host) {
    this.source_host = source_host;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public String getLog_type() {
    return log_type;
  }

  public void setLog_type(String log_type) {
    this.log_type = log_type;
  }

  public String getQd_number() {
    return qd_number;
  }

  public void setQd_number(String qd_number) {
    this.qd_number = qd_number;
  }

  public String getSeat() {
    return seat;
  }

  public void setSeat(String seat) {
    this.seat = seat;
  }

  public String getMarket() {
    return market;
  }

  public void setMarket(String market) {
    this.market = market;
  }

  public String getCap_acc() {
    return cap_acc;
  }

  public void setCap_acc(String cap_acc) {
    this.cap_acc = cap_acc;
  }

  public String getSuborderno() {
    return suborderno;
  }

  public void setSuborderno(String suborderno) {
    this.suborderno = suborderno;
  }

  public String getWt_pnum() {
    return wt_pnum;
  }

  public void setWt_pnum(String wt_pnum) {
    this.wt_pnum = wt_pnum;
  }

  public String getContract_num() {
    return contract_num;
  }

  public void setContract_num(String contract_num) {
    this.contract_num = contract_num;
  }

  @Override
  public String toString() {
    return "NcddztDws{" +
        "source_type='" + source_type + '\'' +
        ", index='" + index + '\'' +
        ", agent_timestamp='" + agent_timestamp + '\'' +
        ", topic='" + topic + '\'' +
        ", file_path='" + file_path + '\'' +
        ", position='" + position + '\'' +
        ", source_host='" + source_host + '\'' +
        ", time=" + time +
        ", log_type='" + log_type + '\'' +
        ", qd_number='" + qd_number + '\'' +
        ", seat='" + seat + '\'' +
        ", market='" + market + '\'' +
        ", cap_acc='" + cap_acc + '\'' +
        ", suborderno='" + suborderno + '\'' +
        ", wt_pnum='" + wt_pnum + '\'' +
        ", contract_num='" + contract_num + '\'' +
        '}';
  }
}
