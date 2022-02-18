package realtime.dao;

public class Mock {
    public long id;
    public String topic;
    public String name;
    public float num;
//    public long ts;

    public Mock() {
    }

    public Mock(long id, String topic, String name, float num) {
        this.id = id;
        this.topic = topic;
        this.name = name;
        this.num = num;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }



    @Override
    public String toString() {
        return "Mock{" +
                "id=" + id +
                ", topic='" + topic + '\'' +
                ", name='" + name + '\'' +
                ", num=" + num +
                '}';
    }
}
