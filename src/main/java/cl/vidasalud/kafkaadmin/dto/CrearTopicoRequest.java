package cl.vidasalud.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.Map;

/** Datos para crear un tópico: nombre, particiones, factor de replicación y configuración (retención, cleanup...). */
public record CrearTopicoRequest(
        @NotBlank @Size(max = 120) @Pattern(regexp = "[A-Za-z0-9._-]+", message = "solo letras, números, punto, guion y guion bajo") String nombre,
        @NotNull @Min(1) @Max(12) Integer particiones,
        @NotNull @Min(1) @Max(3) Integer factorReplicacion,
        Map<String, String> configuracion) {}
