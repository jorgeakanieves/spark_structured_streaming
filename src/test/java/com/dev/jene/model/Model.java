package com.dev.jene.model;

import java.io.Serializable;
import java.sql.Timestamp;

public class Model implements Serializable {
    public String id;
    public Timestamp timeField;
    public Float amount;
    public String country;

    public Model(){}

    public Model(String id, Timestamp timeField, Float amount, String country) {
        this.id = id;
        this.timeField = timeField;
        this.amount = amount;
        this.country = country;

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Timestamp getTimeField() {
        return timeField;
    }

    public void setTimeField(Timestamp timeField) {
        this.timeField = timeField;
    }

    public Float getAmount() {
        return amount;
    }

    public void setAmount(Float amount) {
        this.amount = amount;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}