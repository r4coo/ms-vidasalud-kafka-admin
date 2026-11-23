package cl.vidasalud.kafkaadmin.service;

import cl.vidasalud.kafkaadmin.dto.CrearTopicoRequest;
import cl.vidasalud.kafkaadmin.dto.Respuestas.ClusterInfo;
import cl.vidasalud.kafkaadmin.dto.Respuestas.ConfigInfo;
import cl.vidasalud.kafkaadmin.dto.Respuestas.GrupoDetalle;
import cl.vidasalud.kafkaadmin.dto.Respuestas.GrupoResumen;
import cl.vidasalud.kafkaadmin.dto.Respuestas.NodoInfo;
import cl.vidasalud.kafkaadmin.dto.Respuestas.OffsetGrupo;
import cl.vidasalud.kafkaadmin.dto.Respuestas.ParticionInfo;
import cl.vidasalud.kafkaadmin.dto.Respuestas.TopicoDetalle;
import cl.vidasalud.kafkaadmin.dto.Respuestas.TopicoResumen;
import cl.vidasalud.kafkaadmin.exception.InvalidRequestException;
import cl.vidasalud.kafkaadmin.exception.NotFoundException;
import cl.vidasalud.kafkaadmin.exception.ProtectedTopicException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;

/**
 * Administración del clúster Kafka: tópicos (crear, listar, modificar, eliminar), parámetros
 * (retención, cleanup, particiones, réplicas), grupos de consumidores y su lag.
 */
@Service
public class KafkaAdminService {
    /** Solo estos parámetros se pueden fijar desde la API; el resto del clúster no se toca. */
    static final Set<String> CONFIGS_PERMITIDAS = Set.of("cleanup.policy", "retention.ms", "retention.bytes",
            "segment.ms", "segment.bytes", "min.insync.replicas", "min.cleanable.dirty.ratio",
            "delete.retention.ms", "max.message.bytes");
    static final Set<String> POLITICAS_CLEANUP = Set.of("delete", "compact", "compact,delete", "delete,compact");

    private final Admin admin;

    public KafkaAdminService(Admin admin) {
        this.admin = admin;
    }

    // ---------------------------------------------------------------- tópicos

    public List<TopicoResumen> listarTopicos() {
        Set<String> nombres = esperar(admin.listTopics().names());
        if (nombres.isEmpty()) return List.of();
        Map<String, TopicDescription> descripciones = esperar(admin.describeTopics(nombres).allTopicNames());
        return new TreeMap<>(descripciones).values().stream()
                .map(d -> new TopicoResumen(d.name(), d.partitions().size(), factorReplicacion(d))).toList();
    }

    public TopicoDetalle detalleTopico(String nombre) {
        validarNombre(nombre);
        TopicDescription d = describir(nombre);
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Config config = esperar(admin.describeConfigs(List.of(recurso)).all()).get(recurso);
        List<ConfigInfo> configs = config == null ? List.of() : config.entries().stream()
                .map(e -> new ConfigInfo(e.name(), e.value(),
                        e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG))
                .sorted((a, b) -> a.nombre().compareTo(b.nombre())).toList();
        List<ParticionInfo> particiones = d.partitions().stream()
                .map(p -> new ParticionInfo(p.partition(), p.leader() == null ? null : p.leader().id(),
                        p.replicas().stream().map(Node::id).toList(), p.isr().stream().map(Node::id).toList()))
                .toList();
        return new TopicoDetalle(d.name(), d.partitions().size(), factorReplicacion(d), particiones, configs);
    }

    public TopicoResumen crearTopico(CrearTopicoRequest r) {
        validarNombre(r.nombre());
        if (r.nombre().startsWith("__")) throw new ProtectedTopicException("Los nombres que empiezan con __ están reservados");
        Map<String, String> configs = validarConfiguracion(r.configuracion());
        NewTopic nuevo = new NewTopic(r.nombre(), r.particiones(), r.factorReplicacion().shortValue()).configs(configs);
        esperar(admin.createTopics(List.of(nuevo)).all());
        return new TopicoResumen(r.nombre(), r.particiones(), r.factorReplicacion());
    }

    public TopicoDetalle modificarConfiguracion(String nombre, Map<String, String> cambios) {
        validarNombre(nombre);
        Map<String, String> validados = validarConfiguracion(cambios);
        if (validados.isEmpty()) throw new InvalidRequestException("No hay parámetros para modificar");
        describir(nombre);   // 404 si no existe
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Collection<AlterConfigOp> ops = validados.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .collect(Collectors.toList());
        esperar(admin.incrementalAlterConfigs(Map.of(recurso, ops)).all());
        return detalleTopico(nombre);
    }

    public TopicoDetalle aumentarParticiones(String nombre, int total) {
        validarNombre(nombre);
        int actuales = describir(nombre).partitions().size();
        if (total <= actuales) {
            throw new InvalidRequestException("Kafka solo permite aumentar particiones: el tópico tiene " + actuales
                    + " y se pidió " + total);
        }
        esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(total))).all());
        return detalleTopico(nombre);
    }

    public void eliminarTopico(String nombre) {
        validarNombre(nombre);
        if (nombre.startsWith("__")) throw new ProtectedTopicException("Los tópicos internos de Kafka no se pueden eliminar");
        describir(nombre);   // 404 si no existe
        esperar(admin.deleteTopics(List.of(nombre)).all());
    }

    // ---------------------------------------------------------------- grupos de consumidores

    public List<GrupoResumen> listarGrupos() {
        Collection<org.apache.kafka.clients.admin.ConsumerGroupListing> listado = esperar(admin.listConsumerGroups().all());
        List<String> ids = listado.stream().map(g -> g.groupId()).sorted().toList();
        if (ids.isEmpty()) return List.of();
        Map<String, ConsumerGroupDescription> descripciones = esperar(admin.describeConsumerGroups(ids).all());
        return ids.stream().map(id -> {
            ConsumerGroupDescription d = descripciones.get(id);
            int miembros = d == null ? 0 : d.members().size();
            return new GrupoResumen(id, miembros > 0 ? "ACTIVO" : "SIN_MIEMBROS", miembros);
        }).toList();
    }

    /** Detalle del grupo con offset confirmado, offset final y lag (mensajes pendientes) por partición. */
    public GrupoDetalle detalleGrupo(String grupo) {
        if (grupo == null || grupo.isBlank()) throw new InvalidRequestException("Grupo vacío");
        ConsumerGroupDescription d = esperar(admin.describeConsumerGroups(List.of(grupo)).all()).get(grupo);
        Map<TopicPartition, OffsetAndMetadata> confirmados =
                esperar(admin.listConsumerGroupOffsets(grupo).partitionsToOffsetAndMetadata());
        List<String> miembros = d == null ? List.of() : d.members().stream()
                .map(m -> m.consumerId() + " (" + m.clientId() + "@" + m.host() + ")").toList();
        if (miembros.isEmpty() && confirmados.isEmpty()) {
            throw new NotFoundException("El grupo no existe o no tiene actividad: " + grupo);
        }
        Map<TopicPartition, OffsetSpec> consulta = new HashMap<>();
        confirmados.keySet().forEach(tp -> consulta.put(tp, OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResultInfo> finales =
                consulta.isEmpty() ? Map.of() : esperar(admin.listOffsets(consulta).all());
        List<OffsetGrupo> offsets = new ArrayList<>();
        confirmados.forEach((tp, om) -> {
            long actual = om == null ? 0 : om.offset();
            long fin = finales.containsKey(tp) ? finales.get(tp).offset() : actual;
            offsets.add(new OffsetGrupo(tp.topic(), tp.partition(), actual, fin, Math.max(0, fin - actual)));
        });
        offsets.sort((a, b) -> a.topico().equals(b.topico()) ? Integer.compare(a.particion(), b.particion())
                : a.topico().compareTo(b.topico()));
        long lagTotal = offsets.stream().mapToLong(OffsetGrupo::lag).sum();
        return new GrupoDetalle(grupo, miembros.isEmpty() ? "SIN_MIEMBROS" : "ACTIVO", miembros, offsets, lagTotal);
    }

    // ---------------------------------------------------------------- clúster

    public ClusterInfo cluster() {
        var resultado = admin.describeCluster();
        String id = esperar(resultado.clusterId());
        Node controlador = esperar(resultado.controller());
        Collection<Node> nodos = esperar(resultado.nodes());
        return new ClusterInfo(id, controlador == null ? null : nodo(controlador),
                nodos.stream().map(KafkaAdminService::nodo).sorted((a, b) -> Integer.compare(a.id(), b.id())).toList());
    }

    // ---------------------------------------------------------------- utilidades

    static NodoInfo nodo(Node n) {
        return new NodoInfo(n.id(), n.host(), n.port());
    }

    private static int factorReplicacion(TopicDescription d) {
        return d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size();
    }

    private TopicDescription describir(String nombre) {
        Map<String, TopicDescription> r = esperar(admin.describeTopics(List.of(nombre)).allTopicNames());
        TopicDescription d = r.get(nombre);
        if (d == null) throw new NotFoundException("Tópico inexistente: " + nombre);
        return d;
    }

    static void validarNombre(String nombre) {
        if (nombre == null || !nombre.matches("[A-Za-z0-9._-]{1,120}")) {
            throw new InvalidRequestException("Nombre de tópico inválido: solo letras, números, punto, guion y guion bajo");
        }
    }

    /** Valida los parámetros contra la lista permitida y sus rangos. */
    static Map<String, String> validarConfiguracion(Map<String, String> configs) {
        Map<String, String> validadas = new HashMap<>();
        if (configs == null) return validadas;
        configs.forEach((clave, valor) -> {
            if (!CONFIGS_PERMITIDAS.contains(clave)) {
                throw new InvalidRequestException("Parámetro no permitido: " + clave + ". Permitidos: "
                        + CONFIGS_PERMITIDAS.stream().sorted().toList());
            }
            if (valor == null || valor.isBlank()) throw new InvalidRequestException("Valor vacío para " + clave);
            if ("cleanup.policy".equals(clave)) {
                if (!POLITICAS_CLEANUP.contains(valor)) {
                    throw new InvalidRequestException("cleanup.policy debe ser delete, compact o compact,delete");
                }
            } else if ("min.cleanable.dirty.ratio".equals(clave)) {
                try {
                    double v = Double.parseDouble(valor);
                    if (v < 0 || v > 1) throw new NumberFormatException();
                } catch (NumberFormatException e) {
                    throw new InvalidRequestException("min.cleanable.dirty.ratio debe estar entre 0 y 1");
                }
            } else {
                try {
                    long v = Long.parseLong(valor);
                    if (v < -1 || ("min.insync.replicas".equals(clave) && v < 1)) throw new NumberFormatException();
                } catch (NumberFormatException e) {
                    throw new InvalidRequestException(clave + " debe ser un número entero válido (-1 = sin límite)");
                }
            }
            validadas.put(clave, valor);
        });
        return validadas;
    }

    /** Espera el resultado de Kafka y relanza la causa real (no ExecutionException) para el manejador de errores. */
    static <T> T esperar(KafkaFuture<T> futuro) {
        try {
            return futuro.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new org.apache.kafka.common.errors.InterruptException(e);
        } catch (ExecutionException e) {
            Throwable causa = e.getCause();
            if (causa instanceof RuntimeException re) throw re;
            throw new org.apache.kafka.common.KafkaException(String.valueOf(causa), causa);
        }
    }
}
