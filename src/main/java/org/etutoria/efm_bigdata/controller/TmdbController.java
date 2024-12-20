package org.etutoria.efm_bigdata.controller;

import lombok.RequiredArgsConstructor;
import org.etutoria.efm_bigdata.stream.TmdbService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping("/tmdb")

public class TmdbController {
    private final TmdbService tmdbService;

    public TmdbController(TmdbService tmdbService) {
        this.tmdbService = tmdbService;
    }

    @GetMapping("/consume/all")
    public void consumeAll() {
        List<Integer> movieIds = Arrays.asList(18,35, 550);
        tmdbService.consumeAndPublishAll(movieIds);
    }
}