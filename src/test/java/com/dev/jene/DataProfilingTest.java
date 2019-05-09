package com.dev.jene;

import com.dev.jene.model.Model;
import com.dev.jene.model.WindowConfig;
import com.dev.jene.utils.TestUtils;
import org.apache.spark.sql.*;
import org.junit.*;

import java.io.Serializable;
import java.util.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataProfilingTest implements Serializable {

    private static SparkSession sparkSession;

    @BeforeClass
    public static void setUpClass() throws Exception {
        sparkSession = SparkSession
                .builder()
                .appName("DataProfilingTest")
                .master("local")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        sparkSession.close();
    }

    @Before
    public void setUp() throws Exception {
        assertNotNull(sparkSession.sparkContext());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    @Ignore
    public void testProfiling() {

        String job = "by.activity";
        WindowConfig cfg = new WindowConfig(job);

        ArrayList<Model> transactionsList = new ArrayList<>();
        try {
            transactionsList = TestUtils.generateModels();
        } catch (Exception ex){
            ex.printStackTrace();
        }

        Dataset dsIn = sparkSession.createDataFrame(transactionsList, Model.class);
        dsIn.selectExpr("*").as("json").select("json.*").as(Encoders.bean(Model.class));

        System.out.println();
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>> MODELS Nº: "+  dsIn.collectAsList().size()); //9
        System.out.println();
        dsIn.printSchema();

        DataProfiling.createStatsStream(cfg, dsIn);

        Dataset<Model> dsGrp = DataProfiling.createGroupStream(cfg, dsIn);
        dsGrp.printSchema();
        Dataset slcGrp = DataProfiling.createSelectStream(cfg, dsGrp);

        System.out.println();
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>> WINDOWS Nº: "+ slcGrp.collectAsList().size()); //7
        System.out.println();

        assertTrue(dsGrp.collectAsList().size()>0);

    }


}

