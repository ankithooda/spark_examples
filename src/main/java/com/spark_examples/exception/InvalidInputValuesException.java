package com.spark_examples.exception;

/**
 * Created by ankithooda on 6/9/17.
 */
public class InvalidInputValuesException extends RuntimeException  {
    public InvalidInputValuesException() {
        super("Harmonic Mean is only defined for non-zero values.");
    }
}
