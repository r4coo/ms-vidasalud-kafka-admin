package cl.vidasalud.kafkaadmin.exception;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.KafkaException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Traduce los errores de Kafka y de validación a respuestas HTTP claras. */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static ResponseEntity<Map<String, Object>> respuesta(HttpStatus status, String error, String mensaje) {
        return ResponseEntity.status(status).body(Map.of("error", error, "message", String.valueOf(mensaje)));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<Map<String, Object>> invalid(MethodArgumentNotValidException e) {
        List<String> details = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage()).toList();
        return ResponseEntity.badRequest().body(Map.of("error", "VALIDACION", "message", "Entrada inválida", "details", details));
    }

    @ExceptionHandler({InvalidRequestException.class, InvalidConfigurationException.class,
            InvalidPartitionsException.class, InvalidReplicationFactorException.class, InvalidTopicException.class})
    ResponseEntity<Map<String, Object>> badRequest(RuntimeException e) {
        return respuesta(HttpStatus.BAD_REQUEST, "SOLICITUD_INVALIDA", e.getMessage());
    }

    @ExceptionHandler({NotFoundException.class, UnknownTopicOrPartitionException.class})
    ResponseEntity<Map<String, Object>> notFound(RuntimeException e) {
        return respuesta(HttpStatus.NOT_FOUND, "NO_ENCONTRADO", e.getMessage());
    }

    @ExceptionHandler({TopicExistsException.class, ProtectedTopicException.class})
    ResponseEntity<Map<String, Object>> conflict(RuntimeException e) {
        return respuesta(HttpStatus.CONFLICT, "CONFLICTO", e.getMessage());
    }

    @ExceptionHandler(TimeoutException.class)
    ResponseEntity<Map<String, Object>> timeout(TimeoutException e) {
        return respuesta(HttpStatus.SERVICE_UNAVAILABLE, "KAFKA_NO_RESPONDE", "El clúster Kafka no respondió a tiempo");
    }

    @ExceptionHandler(KafkaException.class)
    ResponseEntity<Map<String, Object>> kafka(KafkaException e) {
        return respuesta(HttpStatus.BAD_GATEWAY, "KAFKA", e.getMessage());
    }
}
