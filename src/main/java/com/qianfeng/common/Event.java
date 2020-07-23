package com.qianfeng.common;

import java.util.Objects;

//封装事件实体对象
public class Event {
    private int id;
    private String name; //事件名称
    private double price;

    public Event(int id, String name, double price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    public double getPrice() {
        return price;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Event(编号→" + id + ",事件名称→ " + name + ", 旅游景点的价格→" + price + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Event) {
            Event other = (Event) obj;

            return name.equals(other.name) && price == other.price && id == other.id;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, price, id);
    }
}