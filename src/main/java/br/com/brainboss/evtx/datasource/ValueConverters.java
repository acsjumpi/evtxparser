package br.com.brainboss.evtx.datasource;

import org.apache.log4j.Logger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

public class ValueConverters {
    private static final Logger log = Logger.getLogger(ValueConverters.class);

    private static final String CUSTOM_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSS";

    public static List<Function> getConverters(StructType schema) {
        StructField[] fields = schema.fields();
        List<Function> valueConverters = new ArrayList<>(fields.length);
        Arrays.stream(fields).forEach(field -> {
            if (field.dataType().equals(DataTypes.StringType)) {
                valueConverters.add(UTF8StringConverter);
            } else if (field.dataType().equals(DataTypes.IntegerType))
                valueConverters.add(IntConverter);
            else if (field.dataType().equals(DataTypes.DoubleType))
                valueConverters.add(DoubleConverter);
            else if (field.dataType().equals(DataTypes.TimestampType))
                valueConverters.add(TimestampConverter);
            else if (field.dataType() instanceof StructType)
                valueConverters.add(StructConverter);
        });
        return valueConverters;
    }


    public static Function<String, UTF8String> UTF8StringConverter = UTF8String::fromString;
    public static Function<String, Double> DoubleConverter = value -> value == null ? null : Double.parseDouble(value);
    public static Function<String, Integer> IntConverter = value -> value == null ? null : Integer.parseInt(value);
    public static Function<StructType, List<Function>> StructConverter = value -> value == null ? null : getConverters(value);
    public static Function<String, Long> TimestampConverter = value -> {
        if (value == null) {
            return null;
        }

        try {
            return new SimpleDateFormat(CUSTOM_FORMAT_STRING).parse(value).toInstant().toEpochMilli() *  1000;
        } catch (ParseException e) {
            log.error("Invalid date format!", e);
            return null;
        }
    };
}