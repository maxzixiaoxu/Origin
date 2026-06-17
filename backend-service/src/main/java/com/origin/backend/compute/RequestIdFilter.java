package com.origin.backend.compute;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Assigns a request id to every incoming request and exposes it for tracing.
 *
 * A filter runs before the controller for every request. We look for an
 * X-Request-Id header (which the load balancer can forward so the same id
 * follows a request end-to-end); if absent we mint a fresh one.
 *
 * The id is placed in the MDC so the logging pattern can print it on every
 * line, and echoed back as a response header so a client (or the LB) can see it.
 */
@Component
@Order(1)
public class RequestIdFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Request-Id";
    private static final String MDC_KEY = "requestId";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain)
            throws ServletException, IOException {

        String requestId = request.getHeader(HEADER);
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString().substring(0, 8);
        }

        MDC.put(MDC_KEY, requestId);
        response.setHeader(HEADER, requestId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            // Always clear the MDC: threads are pooled and reused, so a
            // leftover id would leak into the next unrelated request.
            MDC.remove(MDC_KEY);
        }
    }
}
