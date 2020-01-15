package com.envisioniot.hive.entity;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 计算两点斜率时使用的实体
 * 提供斜率计算方法
 * @author liming.luo
 *
 */
public class Line  implements Comparable<Line>{

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Point first;
    private Point second;
    private Integer type;//1：实线；2：虚线；3：不变

    public Line(){}

    public Line(int type){
        this.type = type;
    }

    public Line(Point first, Point second) {
        super();
        this.first = first;
        this.second = second;
    }

    public Point getFirst() {
        return first;
    }

    public void setFirst(Point first) {
        this.first = first;
    }

    public Point getSecond() {
        return second;
    }

    public void setSecond(Point second) {
        this.second = second;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Double getSlope() {
        //计算发电量两点之间的斜率:电量差（kWh）/时间差(h)
        if(first != null && second != null){
            Double y1 = first.getProd();
            Double y2 = second.getProd();
            Date x1;
            Date x2;
            try {
                x1 = sdf.parse(first.getDate());
                x2 = sdf.parse(second.getDate());
            } catch (ParseException e) {
                e.printStackTrace();
                return -1d;
            }
            if(x1 != null && x2 != null && y1 != null && y2 != null){
                if(x1.getTime() == x2.getTime()){
                    return -1d;
                }
                return (y2 - y1) / ((x2.getTime() - x1.getTime()) * 1.0 / 3600000);
            }
            return -1d;
        }
        return -1d;
    }

    public Double getProd(){
        if(first != null && second != null){
            Double y1 = first.getProd();
            Double y2 = second.getProd();
            if(y1 != null && y2 != null){
                return y2 - y1;
            }
        }
        return 0d;
    }


    public int compareTo(Line other) {
        if(other != null){
            Point otherFirst = other.getFirst();
            if(first != null &&  otherFirst!= null){
                String t1 = first.getDate();
                String t2 = otherFirst.getDate();
                if(t1 != null){
                    return t1.compareTo(t2);
                }
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Line [" + type + ":first=" + first + ", second=" + second + "]";
    }


}
