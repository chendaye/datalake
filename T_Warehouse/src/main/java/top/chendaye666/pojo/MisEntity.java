package top.chendaye666.pojo;

import java.io.Serializable;

public class MisEntity implements Serializable {
    private static final long serialVersionUID = 1978549702447238918L;
    private String field;
    private String type;
    private String value;

    public MisEntity(){}

    public MisEntity(String field, String type, String value) {
        this.field = field;
        this.type = type;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MisEntity{" +
                "field='" + field + '\'' +
                ", type='" + type + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
