package cl.vidasalud.kafkaadmin.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Defensa en profundidad: el BFF ya exige rol Admin (JWT de Entra ID) y propaga
 * los roles en X-User-Roles. Este microservicio rechaza cualquier llamada sin Admin.
 */
public class AdminRoleInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws Exception {
        String roles = req.getHeader("X-User-Roles");
        boolean admin = roles != null && java.util.Arrays.stream(roles.split(",")).map(String::trim).anyMatch("Admin"::equals);
        if (!admin) {
            res.setStatus(HttpServletResponse.SC_FORBIDDEN);
            res.setContentType("application/json");
            res.getWriter().write("{\"error\":\"PROHIBIDO\",\"message\":\"Se requiere el rol Admin\"}");
            return false;
        }
        return true;
    }
}
