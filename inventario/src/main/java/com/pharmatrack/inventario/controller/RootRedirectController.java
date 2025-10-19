package com.pharmatrack.inventario.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/")
public class RootRedirectController {

    @GetMapping({"", "/"})
    public String index() {
        // Redirige a /docs, que está configurado en application.yml
        return "redirect:/docs";
    }
}