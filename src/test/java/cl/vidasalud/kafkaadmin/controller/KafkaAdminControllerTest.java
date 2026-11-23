package cl.vidasalud.kafkaadmin.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import cl.vidasalud.kafkaadmin.config.AdminRoleInterceptor;
import cl.vidasalud.kafkaadmin.dto.Respuestas.TopicoResumen;
import cl.vidasalud.kafkaadmin.exception.GlobalExceptionHandler;
import cl.vidasalud.kafkaadmin.exception.ProtectedTopicException;
import cl.vidasalud.kafkaadmin.service.KafkaAdminService;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class KafkaAdminControllerTest {
    private KafkaAdminService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(KafkaAdminService.class);
        mvc = MockMvcBuilders.standaloneSetup(new KafkaAdminController(service))
                .setControllerAdvice(new GlobalExceptionHandler())
                .addMappedInterceptors(new String[] {"/api/admin/**"}, new AdminRoleInterceptor())
                .build();
    }

    @Test
    void sinRolAdmin_403() throws Exception {
        mvc.perform(get("/api/admin/kafka/topics")).andExpect(status().isForbidden());
        mvc.perform(get("/api/admin/kafka/topics").header("X-User-Roles", "Recepcionista"))
                .andExpect(status().isForbidden());
    }

    @Test
    void conRolAdmin_listaTopicos() throws Exception {
        when(service.listarTopicos()).thenReturn(List.of(new TopicoResumen("vidasalud.atenciones.eventos", 3, 3)));
        mvc.perform(get("/api/admin/kafka/topics").header("X-User-Roles", "Admin"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].nombre").value("vidasalud.atenciones.eventos"))
                .andExpect(jsonPath("$[0].particiones").value(3));
    }

    @Test
    void crearTopico_conParticionesInvalidas_400() throws Exception {
        mvc.perform(post("/api/admin/kafka/topics").header("X-User-Roles", "Admin")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"vidasalud.x\",\"particiones\":0,\"factorReplicacion\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDACION"));
    }

    @Test
    void crearTopico_valido_201() throws Exception {
        when(service.crearTopico(org.mockito.ArgumentMatchers.any()))
                .thenReturn(new TopicoResumen("vidasalud.x", 3, 3));
        mvc.perform(post("/api/admin/kafka/topics").header("X-User-Roles", "Admin")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"vidasalud.x\",\"particiones\":3,\"factorReplicacion\":3}"))
                .andExpect(status().isCreated());
    }

    @Test
    void eliminarTopicoProtegido_409() throws Exception {
        org.mockito.Mockito.doThrow(new ProtectedTopicException("interno")).when(service).eliminarTopico("__x");
        mvc.perform(delete("/api/admin/kafka/topics/__x").header("X-User-Roles", "Admin"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("CONFLICTO"));
    }
}
