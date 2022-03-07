package top.chendaye666.pojo;

/**
 * 解析参数
 */
public class ParamEntity {
    private String type;
    private String field;
    private String strategy;
    private String reg = null;
    private String regIndex = null;

    public ParamEntity(String type, String field, String strategy, String reg, String regIndex) {
        this.type = type;
        this.field = field;
        this.strategy = strategy;
        this.reg = reg;
        this.regIndex = regIndex;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public String getReg() {
        return reg;
    }

    public void setReg(String reg) {
        this.reg = reg;
    }

    public String getRegIndex() {
        return regIndex;
    }

    public void setRegIndex(String regIndex) {
        this.regIndex = regIndex;
    }

    @Override
    public String toString() {
        return "ParamEntity{" +
                "type='" + type + '\'' +
                ", field='" + field + '\'' +
                ", strategy='" + strategy + '\'' +
                ", reg='" + reg + '\'' +
                ", regIndex='" + regIndex + '\'' +
                '}';
    }
}
