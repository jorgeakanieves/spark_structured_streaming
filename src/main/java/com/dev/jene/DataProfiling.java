package com.dev.jene;

import com.dev.jene.model.Model;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.dev.jene.model.WindowConfig;
import com.google.common.base.Preconditions;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;


public class DataProfiling implements KryoSerializable {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DataProfiling.class);
    static Properties prop  = new Properties();
    static String id = UUID.randomUUID().toString();
    static String env = "local";
    static SparkSession sparkSession;

    public static void main (String args[]) throws Exception {

        if (logger.isInfoEnabled())
            logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Init " + DataProfiling.class + " app");

        String configFile = "";
        // env property sent to java
        try {
            if (args.length > 0) {
                configFile = args[0] + ".config.properties";
                env = args[0];
            } else throw new Exception("arg pos 0 env for properties file not sent");
        } catch (Exception ex) {
            logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + ex.getMessage());
            configFile = "local.config.properties";
        }

        InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);
        try {
            prop.load(input);
        } catch (IOException ex) {
            logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + ex.getMessage());
        }

        prop.setProperty("id-process", id);


        if (logger.isInfoEnabled())
            logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Loaded " + configFile);

        prop.entrySet().forEach((e) -> {
            if (logger.isInfoEnabled())
                logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> property key: " + e.getKey() + " -> value: " + e.getValue());
        });

        sparkSession = SparkSession.builder().
            master(prop.get("spark-master").toString()).
            config(ConfigurationOptions.ES_NODES, prop.get("elk-es-nodes").toString()).
            config(ConfigurationOptions.ES_PORT, prop.get("elk-es-port").toString()).
            config(ConfigurationOptions.ES_INDEX_AUTO_CREATE, prop.get("elk-es-index-auto-create").toString()).
            config(ConfigurationOptions.ES_SPARK_DATAFRAME_WRITE_NULL_VALUES, prop.get("elk-es-spark-dataframe-write-null").toString()).
            config(ConfigurationOptions.ES_SPARK_DATAFRAME_WRITE_NULL_VALUES_DEFAULT, prop.get("elk-es-spark-dataframe-write-null-values-default").toString()).
            config(ConfigurationOptions.ES_READ_FIELD_EMPTY_AS_NULL, prop.get("elk-es-read-field-empty-as-null").toString()).
            config(ConfigurationOptions.ES_READ_FIELD_EMPTY_AS_NULL_DEFAULT, prop.get("elk-es-field-read-empty-as-null").toString()).
            config(ConfigurationOptions.ES_NODES_WAN_ONLY, prop.get("elk-es-nodes-wan-only").toString()).
            appName(DataProfiling.class.getName()).getOrCreate();


        sparkSession.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkSession.conf().set("spark.network.timeout", "6000");
        sparkSession.conf().set("spark.rpc.askTimeout", "6000");
        sparkSession.conf().set("spark.akka.timeout", "6000");
        sparkSession.conf().set("spark.worker.timeout", "6000");
        //sparkSession.conf(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
        //sparkSession.conf(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "somepass")

        // parametrization: get from stm
        String standaloneJob = prop.getProperty("jobs-streaming");

        HashMap<String, Dataset<Model>> mapDs = new HashMap<>();
        HashMap<String, StreamingQuery> mapQueryStats = new HashMap<>();
        HashMap<String, StreamingQuery> mapQuery = new HashMap<>();

        String job = standaloneJob;

        if (logger.isInfoEnabled())
            logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Running job: " + job);

        WindowConfig cfg = new WindowConfig(job);

        Dataset dfStream = createStream(cfg);
        mapDs.put(job, dfStream);
        //dfStream.explain(true);
        dfStream.printSchema();

        Dataset groupedStream = createGroupStream(cfg, dfStream);
        groupedStream.printSchema();
        Dataset selectStream = createSelectStream(cfg, groupedStream);

        StreamingQuery query = createOutputGrpStream(cfg, selectStream);

        mapQuery.put(job, query);


        if (logger.isInfoEnabled())
            logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Awaiting termination of job: " + job);

        try {
            mapQuery.get(job).awaitTermination();
            sparkSession.stop();
        } catch (StreamingQueryException ex) {
            logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + ex.getMessage());
            ex.printStackTrace();
        }

    }

    public static Dataset createSelectStream(WindowConfig cfg, Dataset dfStream) {
        // select
        Dataset selStream = dfStream.select(cfg.selectFields);

        if(cfg.showActive == true) {
            selStream.show();
        }
        return selStream;
    }


    public static StreamingQuery createStatsStream(WindowConfig cfg, Dataset dfStream) {

        // output
        if(cfg.showActive == true) {
            dfStream.stat().crosstab(cfg.crossTab1, cfg.crossTab2).show();
        } else {
            //TODO: include stats
            return dfStream.stat().crosstab(cfg.crossTab1, cfg.crossTab2).writeStream()
                .outputMode(cfg.outputMode)
                .format(cfg.outputFormat)
                .option("truncate", "false")
                .option("checkpointLocation", cfg.checkPointLocation)
                .trigger(Trigger.ProcessingTime(cfg.triggerTime))
                .start(cfg.outputStartStats);
        }
        return null;
    }


    public static StreamingQuery createOutputGrpStream(WindowConfig cfg, Dataset groupedStream) {

        if(cfg.showActive == true) {
            groupedStream.show();
        } else {
            //TODO: triggering is not working properly for showing data
            // output
            StreamingQuery q = groupedStream.writeStream()
                    .outputMode(cfg.outputMode)
                    .format(cfg.outputFormat)
                    .option("truncate", "false")
                    .option("checkpointLocation", cfg.checkPointLocation)
                    .trigger(Trigger.ProcessingTime(Long.parseLong(cfg.triggerTime)))
                    .start(cfg.outputStart);

            return q;
        }
        return null;
    }

    public static Dataset createGroupStream(WindowConfig cfg, Dataset dfStream) {

        // aggregations
        List<Column> aggrExprs = cfg.aggregations.stream().map(expr -> expr(expr)).collect(Collectors.toList());
        Preconditions.checkState(aggrExprs.size() > 0);
        Column headAggrExprs = aggrExprs.get(0);
        Column[] tailAggrExprs = aggrExprs.subList(1, aggrExprs.size()).stream().toArray(Column[]::new);

        // groupByFields
        List<Optional> listGroupByFields = new ArrayList<Optional>();
        cfg.groupByFields.forEach(i -> listGroupByFields.add(Optional.of(i).map(f -> col(f))));
        Stream streamGroupBy = listGroupByFields.stream();

        List<Optional> listGroupByKey = new ArrayList<Optional>();
        cfg.groupByKeyField.forEach(i -> listGroupByKey.add(Optional.of(i).map(f -> col(f))));
        Stream streamGroupByKey = listGroupByKey.stream();

        // groupby fields
        Column window = functions.window(col(cfg.windowField), cfg.windowDuration, cfg.slideDuration);
        //Optional<Column> groupByKeyColumn = cfg.groupByKeyField.map(f -> col(f));
        Stream<Optional<Column>> groupByStream = Stream.concat(Stream.concat(Stream.of(Optional.of(window)), streamGroupBy), streamGroupByKey);

        //Stream<Column> groupByColumns = Stream.of(groupByKeyColumn, Optional.of(window), groupByKeyField2)
        Stream<Column> groupByColumns = groupByStream
                .filter(Optional::isPresent)
                .map(Optional::get);


        // query stream
        return dfStream
                .withWatermark(cfg.watermarkField, cfg.watermarkDuration)
                .groupBy(groupByColumns.toArray(Column[]::new))
                .agg(headAggrExprs, tailAggrExprs)
                ;

    }

    public static Dataset createStream(WindowConfig cfg){

        Class modelClass = Model.class;
        try {
            modelClass = Class.forName(cfg.objectClass).newInstance().getClass();
        } catch (Exception ex) {
            logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + ex.getMessage());
            ex.printStackTrace();
        }

        // input
        // model streaming schema
        StructType schema = Encoders.bean(modelClass).schema();
        return sparkSession
                        .readStream()
                        .format(prop.get("kafka-format").toString())
                        .option("kafka.bootstrap.servers", prop.get("kafka.bootstrap.servers").toString())
                        .option("failOnDataLoss", prop.get("kafka-fail-data-loss").toString())
                        .option("startingOffsets", "latest")
                        .option("includeTimestamp", true)
                        .option("subscribe", cfg.inputTopic)
                        .load()
                        .selectExpr("CAST(value AS STRING) as message")
                        .select(functions.from_json(functions.col("message"), schema).as("json"))
                        .select("json.*").as(Encoders.bean(modelClass));



    }

    @Override
    public void write(Kryo kryo, Output output) {

    }

    @Override
    public void read(Kryo kryo, Input input) {

    }
}
