package com.envisioniot.hive.entity;

/**
 * 计算发电量时实体
 * date : x
 * prod : y
 *
 */
public class Point implements Comparable<Point> {

    private String date;
    private Double prod;

    public Point(){

    }

    public Point(String date, Double prod) {
        super();
        this.date = date;
        this.prod = prod;
    }

    public String getDate() {
        return date;
    }
    public void setDate(String date) {
        this.date = date;
    }
    public Double getProd() {
        return prod;
    }
    public void setProd(Double prod) {
        this.prod = prod;
    }

    public int compareTo(Point other) {
        if(other != null){
            if(date != null){
                return date.compareTo(other.getDate());
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return date + "," + prod;
    }

}
