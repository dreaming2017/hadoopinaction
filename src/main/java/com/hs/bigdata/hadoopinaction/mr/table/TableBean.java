package com.hs.bigdata.hadoopinaction.mr.table;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 创建商品和订合并后的Bean类
 */
public class TableBean implements Writable {

    private String orderId; // 订单id
    private String pId;      // 产品id
    private int amount;       // 产品数量
    private String pname;     // 产品名称
    private String flag;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(pId);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.pId = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.flag = dataInput.readUTF();

    }

    public TableBean() {
    super();
    }

    public TableBean(String orderId, String pId, int amount, String pname, String flag) {
        this.orderId = orderId;
        this.pId = pId;
        this.amount = amount;
        this.pname = pname;
        this.flag = flag;
    }


    @Override
    public String toString() {
        return orderId + "\t" + pname + "\t" + amount + "\t" ;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
