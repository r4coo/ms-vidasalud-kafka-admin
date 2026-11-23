package cl.vidasalud.kafkaadmin.controller;

import cl.vidasalud.kafkaadmin.dto.CambiarConfiguracionRequest;
import cl.vidasalud.kafkaadmin.dto.CambiarParticionesRequest;
import cl.vidasalud.kafkaadmin.dto.CrearTopicoRequest;
import cl.vidasalud.kafkaadmin.dto.Respuestas.ClusterInfo;
import cl.vidasalud.kafkaadmin.dto.Respuestas.GrupoDetalle;
import cl.vidasalud.kafkaadmin.dto.Respuestas.GrupoResumen;
import cl.vidasalud.kafkaadmin.dto.Respuestas.TopicoDetalle;
import cl.vidasalud.kafkaadmin.dto.Respuestas.TopicoResumen;
import cl.vidasalud.kafkaadmin.service.KafkaAdminService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** API de administración de Kafka (solo rol Admin, validado por AdminRoleInterceptor). */
@RestController
@RequestMapping("/api/admin/kafka")
public class KafkaAdminController {
    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/cluster")
    public ClusterInfo cluster() { return service.cluster(); }

    @GetMapping("/topics")
    public List<TopicoResumen> topicos() { return service.listarTopicos(); }

    @GetMapping("/topics/{nombre}")
    public TopicoDetalle topico(@PathVariable String nombre) { return service.detalleTopico(nombre); }

    @PostMapping("/topics")
    public ResponseEntity<TopicoResumen> crear(@Valid @RequestBody CrearTopicoRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(service.crearTopico(request));
    }

    @PutMapping("/topics/{nombre}/config")
    public TopicoDetalle configuracion(@PathVariable String nombre, @Valid @RequestBody CambiarConfiguracionRequest r) {
        return service.modificarConfiguracion(nombre, r.configuracion());
    }

    @PutMapping("/topics/{nombre}/partitions")
    public TopicoDetalle particiones(@PathVariable String nombre, @Valid @RequestBody CambiarParticionesRequest r) {
        return service.aumentarParticiones(nombre, r.total());
    }

    @DeleteMapping("/topics/{nombre}")
    public ResponseEntity<Map<String, String>> eliminar(@PathVariable String nombre) {
        service.eliminarTopico(nombre);
        return ResponseEntity.ok(Map.of("mensaje", "Tópico eliminado: " + nombre));
    }

    @GetMapping("/groups")
    public List<GrupoResumen> grupos() { return service.listarGrupos(); }

    @GetMapping("/groups/{grupo}")
    public GrupoDetalle grupo(@PathVariable String grupo) { return service.detalleGrupo(grupo); }
}
