package cl.vidasalud.kafkaadmin.dto;

import jakarta.validation.constraints.NotEmpty;
import java.util.Map;

/** Parámetros del tópico a modificar (retention.ms, cleanup.policy, ...). */
public record CambiarConfiguracionRequest(@NotEmpty Map<String, String> configuracion) {}
