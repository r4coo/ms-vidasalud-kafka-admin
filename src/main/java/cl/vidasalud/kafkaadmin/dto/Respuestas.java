package cl.vidasalud.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

/** Respuestas de la API de administración de Kafka. */
public final class Respuestas {
    private Respuestas() {}

    public record TopicoResumen(String nombre, int particiones, int factorReplicacion) {}

    public record ParticionInfo(int particion, Integer lider, List<Integer> replicas, List<Integer> isr) {}

    public record ConfigInfo(String nombre, String valor, boolean personalizada) {}

    public record TopicoDetalle(String nombre, int particiones, int factorReplicacion,
                                List<ParticionInfo> detalleParticiones, List<ConfigInfo> configuracion) {}

    public record GrupoResumen(String grupo, String estado, int miembros) {}

    public record OffsetGrupo(String topico, int particion, long offsetActual, long offsetFinal, long lag) {}

    public record GrupoDetalle(String grupo, String estado, List<String> miembros, List<OffsetGrupo> offsets,
                               long lagTotal) {}

    public record NodoInfo(int id, String host, int puerto) {}

    public record ClusterInfo(String clusterId, NodoInfo controlador, List<NodoInfo> brokers) {}

    public record Mensaje(String mensaje, Map<String, Object> datos) {}
}
