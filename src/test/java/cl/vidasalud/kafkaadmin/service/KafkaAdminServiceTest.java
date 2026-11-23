package cl.vidasalud.kafkaadmin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.vidasalud.kafkaadmin.dto.CrearTopicoRequest;
import cl.vidasalud.kafkaadmin.exception.InvalidRequestException;
import cl.vidasalud.kafkaadmin.exception.NotFoundException;
import cl.vidasalud.kafkaadmin.exception.ProtectedTopicException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class KafkaAdminServiceTest {
    private Admin admin;
    private KafkaAdminService service;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        service = new KafkaAdminService(admin);
    }

    @Test
    void crear_enviaParticionesReplicasYConfiguracion() {
        CreateTopicsResult resultado = mock(CreateTopicsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(any(Collection.class))).thenReturn(resultado);

        var creado = service.crearTopico(new CrearTopicoRequest("vidasalud.prueba", 3, 3,
                Map.of("retention.ms", "604800000", "cleanup.policy", "delete")));

        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(captor.capture());
        NewTopic enviado = captor.getValue().iterator().next();
        assertEquals("vidasalud.prueba", enviado.name());
        assertEquals(3, enviado.numPartitions());
        assertEquals((short) 3, enviado.replicationFactor());
        assertEquals("604800000", enviado.configs().get("retention.ms"));
        assertEquals(3, creado.particiones());
    }

    @Test
    void crear_topicoExistente_propagaLaCausaReal() {
        CreateTopicsResult resultado = mock(CreateTopicsResult.class);
        var fallo = new org.apache.kafka.common.internals.KafkaFutureImpl<Void>();
        fallo.completeExceptionally(new TopicExistsException("ya existe"));
        when(resultado.all()).thenReturn(fallo);
        when(admin.createTopics(any(Collection.class))).thenReturn(resultado);

        assertThrows(TopicExistsException.class,
                () -> service.crearTopico(new CrearTopicoRequest("vidasalud.prueba", 3, 3, null)));
    }

    @Test
    void crear_nombreReservado_seRechazaSinLlamarAKafka() {
        assertThrows(ProtectedTopicException.class,
                () -> service.crearTopico(new CrearTopicoRequest("__interno", 1, 1, null)));
        verify(admin, never()).createTopics(any(Collection.class));
    }

    @Test
    void crear_parametroNoPermitido_seRechaza() {
        assertThrows(InvalidRequestException.class, () -> service.crearTopico(
                new CrearTopicoRequest("vidasalud.prueba", 3, 3, Map.of("unclean.leader.election.enable", "true"))));
    }

    @Test
    void validarConfiguracion_rangosYPoliticas() {
        assertEquals(Map.of("retention.ms", "-1"), KafkaAdminService.validarConfiguracion(Map.of("retention.ms", "-1")));
        assertThrows(InvalidRequestException.class,
                () -> KafkaAdminService.validarConfiguracion(Map.of("retention.ms", "abc")));
        assertThrows(InvalidRequestException.class,
                () -> KafkaAdminService.validarConfiguracion(Map.of("cleanup.policy", "otra")));
        assertThrows(InvalidRequestException.class,
                () -> KafkaAdminService.validarConfiguracion(Map.of("min.cleanable.dirty.ratio", "2")));
        assertThrows(InvalidRequestException.class,
                () -> KafkaAdminService.validarConfiguracion(Map.of("min.insync.replicas", "0")));
        assertEquals(Map.of(), KafkaAdminService.validarConfiguracion(null));
    }

    @Test
    void validarNombre_rechazaCaracteresInvalidos() {
        assertThrows(InvalidRequestException.class, () -> KafkaAdminService.validarNombre("../etc"));
        assertThrows(InvalidRequestException.class, () -> KafkaAdminService.validarNombre(" "));
        KafkaAdminService.validarNombre("vidasalud.atenciones.eventos");
    }

    @Test
    void eliminar_topicoInterno_estaProtegido() {
        assertThrows(ProtectedTopicException.class, () -> service.eliminarTopico("__consumer_offsets"));
        verify(admin, never()).deleteTopics(any(Collection.class));
    }

    @Test
    void eliminar_topicoInexistente_esNoEncontrado() {
        DescribeTopicsResult resultado = mock(DescribeTopicsResult.class);
        when(resultado.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.describeTopics(any(Collection.class))).thenReturn(resultado);
        assertThrows(NotFoundException.class, () -> service.eliminarTopico("no.existe"));
    }

    @Test
    void aumentarParticiones_noPermiteReducir() {
        DescribeTopicsResult resultado = mock(DescribeTopicsResult.class);
        var particiones = List.of(
                new org.apache.kafka.common.TopicPartitionInfo(0, null, List.of(), List.of()),
                new org.apache.kafka.common.TopicPartitionInfo(1, null, List.of(), List.of()),
                new org.apache.kafka.common.TopicPartitionInfo(2, null, List.of(), List.of()));
        var descripcion = new org.apache.kafka.clients.admin.TopicDescription("t", false, particiones);
        when(resultado.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("t", descripcion)));
        when(admin.describeTopics(any(Collection.class))).thenReturn(resultado);

        assertThrows(InvalidRequestException.class, () -> service.aumentarParticiones("t", 3));
        assertThrows(InvalidRequestException.class, () -> service.aumentarParticiones("t", 2));
    }

    @Test
    void listarTopicos_sinTopicos_devuelveListaVacia() {
        var listado = mock(org.apache.kafka.clients.admin.ListTopicsResult.class);
        when(listado.names()).thenReturn(KafkaFuture.completedFuture(java.util.Set.of()));
        when(admin.listTopics()).thenReturn(listado);
        assertEquals(List.of(), service.listarTopicos());
    }
}
