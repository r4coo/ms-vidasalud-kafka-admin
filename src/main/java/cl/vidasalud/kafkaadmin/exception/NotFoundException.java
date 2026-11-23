package cl.vidasalud.kafkaadmin.exception;

public class NotFoundException extends RuntimeException {
    public NotFoundException(String message) { super(message); }
}
