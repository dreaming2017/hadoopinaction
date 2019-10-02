package com.hs.bigdata.hadoopinaction.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;        // 订单id
    private String productId;
    private Double price;        // 价格

    @Override
    public int compareTo(OrderBean o) {
        int compare = this.orderId.compareTo(o.orderId);
        if(compare==0){
            return Double.compare(o.price,this.price);
        }
        return compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(productId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.productId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    @Override
    public String toString() {
        return (orderId + "\t" + productId + "\t" + price);
    }
}
