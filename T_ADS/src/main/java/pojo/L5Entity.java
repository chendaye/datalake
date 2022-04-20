package pojo;

import java.io.Serializable;

public class L5Entity implements Serializable {
    private static final long serialVersionUID = 8151027747503164771L;
    // EZOES 确认委托耗时
    long time;
    // 合同号 channel
    String contractNo;
    // 清单号 channel2
    String listNumber;
    // 资金号 var_str
    String fundNumber;
    // 席位号 node
    String seatNumber;

    public L5Entity(){}

    public L5Entity(long time, String contractNo, String listNumber, String fundNumber, String seatNumber) {
        this.time = time;
        this.contractNo = contractNo;
        this.listNumber = listNumber;
        this.fundNumber = fundNumber;
        this.seatNumber = seatNumber;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public long getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public String getContractNo() {
        return contractNo;
    }

    public void setContractNo(String contractNo) {
        this.contractNo = contractNo;
    }

    public String getListNumber() {
        return listNumber;
    }

    public void setListNumber(String listNumber) {
        this.listNumber = listNumber;
    }

    public String getFundNumber() {
        return fundNumber;
    }

    public void setFundNumber(String fundNumber) {
        this.fundNumber = fundNumber;
    }

    public String getSeatNumber() {
        return seatNumber;
    }

    public void setSeatNumber(String seatNumber) {
        this.seatNumber = seatNumber;
    }

    @Override
    public String toString() {
        return "L5Entity{" +
                "time=" + time +
                ", contractNo='" + contractNo + '\'' +
                ", listNumber='" + listNumber + '\'' +
                ", fundNumber='" + fundNumber + '\'' +
                ", seatNumber='" + seatNumber + '\'' +
                '}';
    }
}
