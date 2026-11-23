package cl.vidasalud.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/** Nuevo total de particiones (Kafka solo permite aumentarlas). */
public record CambiarParticionesRequest(@NotNull @Min(1) @Max(12) Integer total) {}
