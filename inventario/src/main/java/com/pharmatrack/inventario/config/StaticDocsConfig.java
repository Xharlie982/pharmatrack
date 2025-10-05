package com.pharmatrack.inventario.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class StaticDocsConfig {

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
