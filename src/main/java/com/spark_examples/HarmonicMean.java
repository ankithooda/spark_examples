package com.spark_examples;

import java.util.ArrayList;
import java.util.List;

import com.spark_examples.exception.InvalidInputValuesException;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Created by ankithooda on 6/9/17.
 */
public class HarmonicMean extends UserDefinedAggregateFunction{

    public StructType inputSchema() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("input", DataTypes.DoubleType, true));
        return DataTypes.createStructType(inputFields);
    }

    public StructType bufferSchema() {
        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("sum", DataTypes.DoubleType, true));
        bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
        return DataTypes.createStructType(bufferFields);
    }

    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    public boolean deterministic() {
        return true;
    }

    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0.0);
        buffer.update(1, 0L);
    }

    public void update(MutableAggregationBuffer buffer, Row input) {
        if (!input.isNullAt(0)) {
            double inputValue = input.getDouble(0);
            if (inputValue <= 0) {
                throw new InvalidInputValuesException();
            }
            double bufferSum = buffer.getDouble(0);
            long bufferCount = buffer.getLong(1);

            buffer.update(0, bufferSum + (1 / inputValue));
            buffer.update(1, bufferCount + 1);
        }
    }

    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0));
        buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
    }

    public Double evaluate(Row buffer) {
        return ((double) buffer.getLong(1)) / buffer.getDouble(0);
    }
}
