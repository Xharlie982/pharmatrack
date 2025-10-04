package com.pharmatrack.inventario;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Arrays;

@SpringBootApplication
public class InventarioApp {

  public static void main(String[] args) {
    SpringApplication.run(InventarioApp.class, args);
  }

  /**
   * CORS global configurable.
   * - Lee "cors.origins" del application.yml (e.g. "*", "https://ui1,https://ui2").
   * - Si es "*", usa allowedOriginPatterns("*") para que Spring haga echo del Origin
   *   (compatible con allowCredentials(true)).
   */
  @Bean
  public WebMvcConfigurer corsConfigurer(
          @Value("${cors.origins:*}") String originsProp
  ) {
    return new WebMvcConfigurer() {
      @Override
      public void addCorsMappings(CorsRegistry registry) {
        String prop = originsProp == null ? "*" : originsProp.trim();
        if ("*".equals(prop)) {
          registry.addMapping("/**")
                  .allowedOriginPatterns("*")
                  .allowedMethods("*")
                  .allowedHeaders("*")
                  .allowCredentials(true);
        } else {
          String[] origins = Arrays.stream(prop.split(","))
                  .map(String::trim)
                  .filter(s -> !s.isEmpty())
                  .toArray(String[]::new);

          registry.addMapping("/**")
                  .allowedOrigins(origins)
                  .allowedMethods("*")
                  .allowedHeaders("*")
                  .allowCredentials(true);
        }
      }
    };
  }

  /**
   * Sirve archivos estáticos bajo /docs/** desde un directorio del filesystem
   * (montado en el contenedor), sin necesidad de resources/static.
   * DOCS_DIR proviene de env (compose) y cae en /app/docs por defecto.
   */
  @Bean
  public WebMvcConfigurer staticDocs(@Value("${docs.dir:/app/docs}") String docsDir) {
    return new WebMvcConfigurer() {
      @Override
      public void addResourceHandlers(ResourceHandlerRegistry registry) {
        String base = docsDir.endsWith("/") ? docsDir : docsDir + "/";
        registry.addResourceHandler("/docs/**")
                .addResourceLocations("file:" + base);
      }
    };
  }
}
