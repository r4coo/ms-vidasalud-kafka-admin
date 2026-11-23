package cl.vidasalud.kafkaadmin.exception;

public class ProtectedTopicException extends RuntimeException {
    public ProtectedTopicException(String message) { super(message); }
}
