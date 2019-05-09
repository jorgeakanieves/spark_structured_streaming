package com.dev.jene.model;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class WindowConfig {
    public String job;
    public String objectClass; // "eventTime"
    public String watermarkField; // "eventTime"
    public String watermarkDuration; // "1 minute"

    public String windowField; // "eventTime"
    public String windowDuration; // "10 minutes"
    public String slideDuration; // 5 minutes"
    //public Optional<String> groupByKeyField; // "country"
    public List<String> groupByKeyField;
    public List<String> groupByFields;
    //public String aggregations_old; // "avg(amount), sum(amount), max(amount), min(amount)"
    public List<String> aggregations;

    public String outputMode;
    public String outputFormat;
    public String triggerTime;
    public String outputStart;
    public String checkPointLocation;
    public String inputTopic;
    public boolean showActive;
    public String crossTab1;
    public String crossTab2;
    public String selectFields;
    public String outputStartStats;


    static Properties prop  = new Properties();
    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(WindowConfig.class);

    public WindowConfig(String job) {
        this.job = job;
        this.loadConfig();
    }
    
    public void loadConfig(){

        this.loadConfigFile();

        this.objectClass = prop.get("object-class").toString();
        this.watermarkField = prop.get("watermark-field").toString();
        this.watermarkDuration =  prop.get("watermark-duration").toString();
        this.windowField = prop.get("window-field").toString();
        this.windowDuration = prop.get("window-duration").toString();
        this.slideDuration = prop.get("slide-duration").toString();
        this.groupByKeyField = Pattern.compile(", ").splitAsStream(prop.get("group-by-key-field").toString()).map(String::trim).collect(Collectors.toList());
        this.groupByFields = Pattern.compile(", ").splitAsStream(prop.get("group-by-fields").toString()).map(String::trim).collect(Collectors.toList());
        this.aggregations = Lists.newArrayList(prop.get("aggregations").toString().split(","));
        this.showActive = Boolean.valueOf(prop.get("show-active").toString());
        this.outputMode = prop.get("output-mode").toString();
        this.outputFormat = prop.get("output-format").toString();
        this.triggerTime = prop.get("trigger-time").toString();
        this.outputStart = prop.get("output-start").toString();
        this.checkPointLocation = prop.get("check-point-location").toString();
        this.inputTopic = prop.get("kafka-topic").toString();
        this.crossTab1 = prop.get("cross-tab-1").toString();
        this.crossTab2 = prop.get("cross-tab-2").toString();
        this.selectFields = prop.get("select-fields").toString();
        this.outputStartStats = prop.get("output-start-stats").toString();

    }

    public void loadConfigFile() {

        String windowingConfFile = "job."+ this.job + ".windowing.properties";
        InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(windowingConfFile);
        try {
            prop.load(input);
        } catch (IOException ex) {
            logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + ex.getMessage());
        }
        if (logger.isInfoEnabled())
            logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Loaded windowing config file " + windowingConfFile);

        prop.entrySet().forEach((e) -> {
            if (logger.isInfoEnabled())
                logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> property key: " + e.getKey() + " -> value: " + e.getValue());
        });
    }
}