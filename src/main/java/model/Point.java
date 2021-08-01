package model;

import java.io.Serializable;

public class Point implements Serializable {
    private double x, y;
    private Integer id = null, count = 1;

    public Point(String line) {
        String[] tokens = line.split(",");
        x = Double.parseDouble(tokens[0]);
        y = Double.parseDouble(tokens[1]);
    }

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point(double x, double y, int id) {
        this.x = x;
        this.y = y;
        this.id = id;
    }


    public Point normalize() {
        x = x / count;
        y = y / count;
        return this;
    }

    public double getDistance(Point p) {
        double xDiff = Math.abs(this.x - p.x);
        double yDiff = Math.abs(this.y - p.y);
        return Math.sqrt(Math.pow(xDiff, 2) + Math.pow(yDiff, 2));
    }


    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public String toString() {
        return  x + "," + y + "," + count;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getId() {
        return id;
    }
}
