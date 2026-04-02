package com.morocco.events.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CityStatistics implements Serializable {
    private String city;
    private long totalEvents;
    private double avgPrice;
    private double maxPrice;
    private double minPrice;
    private Map<String, Long> categoryCount;

    public CityStatistics() {
        this.categoryCount = new HashMap<>();
    }

    public CityStatistics(String city) {
        this.city = city;
        this.totalEvents = 0;
        this.avgPrice = 0.0;
        this.maxPrice = 0.0;
        this.minPrice = Double.MAX_VALUE;
        this.categoryCount = new HashMap<>();
    }

    // Getters et Setters
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public long getTotalEvents() { return totalEvents; }
    public void setTotalEvents(long totalEvents) { this.totalEvents = totalEvents; }

    public double getAvgPrice() { return avgPrice; }
    public void setAvgPrice(double avgPrice) { this.avgPrice = avgPrice; }

    public double getMaxPrice() { return maxPrice; }
    public void setMaxPrice(double maxPrice) { this.maxPrice = maxPrice; }

    public double getMinPrice() { return minPrice; }
    public void setMinPrice(double minPrice) { this.minPrice = minPrice; }

    public Map<String, Long> getCategoryCount() { return categoryCount; }
    public void setCategoryCount(Map<String, Long> categoryCount) {
        this.categoryCount = categoryCount;
    }

    @Override
    public String toString() {
        return "CityStatistics{" +
                "city='" + city + '\'' +
                ", totalEvents=" + totalEvents +
                ", avgPrice=" + String.format("%.2f", avgPrice) +
                ", maxPrice=" + String.format("%.2f", maxPrice) +
                ", minPrice=" + String.format("%.2f", minPrice) +
                ", categoryCount=" + categoryCount +
                '}';
    }
}