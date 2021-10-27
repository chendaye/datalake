package top.chendaye666;

public class TestPo {
    int id;
    String data;

    public TestPo(int id, String data) {
        this.id = id;
        this.data = data;
    }

    public TestPo() {
    }

    @Override
    public String toString() {
        return "TestPo{" +
                "id=" + id +
                ", data='" + data + '\'' +
                '}';
    }
}
