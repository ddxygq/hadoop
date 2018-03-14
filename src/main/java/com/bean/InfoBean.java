package com.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2018/3/6.
 */
public class InfoBean implements Writable {

    private int order_id;
    private String date;
    private String p_id;
    private int amount;
    private String pname;
    private int category_id;
    private float price;
    private String flag;

    public int getOrder_id() {
        return order_id;
    }

    public String getDate() {
        return date;
    }

    public String getP_id() {
        return p_id;
    }

    public int getAmount() {
        return amount;
    }

    public String getPname() {
        return pname;
    }

    public int getCategory_id() {
        return category_id;
    }

    public float getPrice() {
        return price;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getFlag() {

        return flag;
    }

    public InfoBean() {
    }

    public void set(int order_id, String date, String p_id, int amount, String pname, int category_id, float price,String flag) {
        this.order_id = order_id;
        this.date = date;
        this.p_id = p_id;
        this.amount = amount;
        this.pname = pname;
        this.category_id = category_id;
        this.price = price;
        this.flag = flag;
    }

    /**
     * private int order_id;
     private String date;
     private int p_id;
     private int amount;
     private String pname;
     private int category_id;
     private float price;
     * @param out
     * @throws IOException
     */

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeUTF(date);
        out.writeUTF(p_id);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeInt(category_id);
        out.writeFloat(price);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readInt();
        this.date = in.readUTF();
        this.p_id = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
        this.category_id = in.readInt();
        this.price = in.readFloat();
        this.flag = in.readUTF();
    }

    @Override
    public String toString() {
        return "order_id=" + order_id +
                ", date='" + date + '\'' +
                ", p_id=" + p_id +
                ", amount=" + amount +
                ", pname='" + pname + '\'' +
                ", category_id=" + category_id +
                ", price=" + price +
                ", flag='" + flag;
    }
}
