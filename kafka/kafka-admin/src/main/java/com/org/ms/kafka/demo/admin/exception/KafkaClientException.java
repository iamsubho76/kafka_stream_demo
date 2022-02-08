package com.org.ms.kafka.demo.admin.exception;

/**
 * Exception class for Kafka client error situations.
 */
public class KafkaClientException extends RuntimeException {

    public KafkaClientException() {
    }

    public KafkaClientException(String message) {
        super(message);
    }

    public KafkaClientException(String message, Throwable cause) {
        super(message, cause);
    }
}