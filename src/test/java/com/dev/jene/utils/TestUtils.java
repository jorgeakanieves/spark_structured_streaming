package com.dev.jene.utils;

import com.dev.jene.model.Model;
import com.google.gson.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Scanner;

public class TestUtils {


    public static ArrayList<Model> generateModels() throws ParseException {

        // Prepare Input Topic
        File file = new File(Thread.currentThread().getContextClassLoader().getResource("models.json")
                .getFile());
        try {
            Scanner sc = new Scanner(file);
            sc.close();
        } catch (Exception ex){
            ex.printStackTrace();
        }

        ArrayList<String> data = new ArrayList<String>();
        ArrayList<Model> transactionList = new ArrayList<Model>();
        try (Scanner scanner = new Scanner(file)) {

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                data.add(line);

                Gson gson = new GsonBuilder().registerTypeAdapter(Date.class, ser)
                        .registerTypeAdapter(Date.class, deser).registerTypeAdapter(Timestamp.class, ser2)
                        .registerTypeAdapter(Timestamp.class, deser2).create();

                //Gson gson = new GsonBuilder().create();

                Model transaction = (Model) gson.fromJson(line, Model.class);
                transactionList.add(transaction);
            }

            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return transactionList;
    }



    static JsonSerializer<Date> ser = new JsonSerializer<Date>() {
        @Override
        public JsonElement serialize(Date src, Type typeOfSrc, JsonSerializationContext
                context) {
            return src == null ? null : new JsonPrimitive(src.getDate());
        }
    };

    static JsonDeserializer<Date> deser = new JsonDeserializer<Date>() {
        @Override
        public Date deserialize(JsonElement json, Type typeOfT,
                                JsonDeserializationContext context) throws JsonParseException {


            java.util.Date date = new java.util.Date();
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            try {
                date = (java.util.Date) formatter.parse(json.getAsString());
            } catch(ParseException ex){}

            java.sql.Date sqlDate;
            sqlDate = new java.sql.Date(date.getTime());
            return json == null ? null : sqlDate;

        }
    };

    static JsonSerializer<Timestamp> ser2 = new JsonSerializer<Timestamp>() {
        @Override
        public JsonElement serialize(Timestamp src, Type typeOfSrc, JsonSerializationContext
                context) {
            return src == null ? null : new JsonPrimitive(src.getTime());
        }
    };

    static JsonDeserializer<Timestamp> deser2 = new JsonDeserializer<Timestamp>() {
        @Override
        public Timestamp deserialize(JsonElement json, Type typeOfT,
                                     JsonDeserializationContext context) throws JsonParseException {

            return json == null ? null : new Timestamp(json.getAsLong());
        }
    };

}
