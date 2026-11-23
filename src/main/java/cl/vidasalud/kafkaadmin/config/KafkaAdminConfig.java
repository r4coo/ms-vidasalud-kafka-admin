package cl.vidasalud.kafkaadmin.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Cliente administrativo de Kafka (AdminClient) contra los 3 brokers. */
@Configuration
public class KafkaAdminConfig {

    @Bean(destroyMethod = "close")
    public Admin kafkaAdmin(@Value("${app.kafka.bootstrap-servers}") String bootstrapServers,
                            @Value("${app.kafka.request-timeout-ms}") int timeoutMs) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        cfg.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
        return Admin.create(cfg);
    }
}
