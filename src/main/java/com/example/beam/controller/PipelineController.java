package com.example.beam.controller;

import com.example.beam.BeamService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/pipeline")
public class PipelineController {

    private final BeamService beamService;

    public PipelineController(BeamService beamService) {
        this.beamService = beamService;
    }

    @PostMapping("/run")
    public ResponseEntity<String> runPipeline() throws Exception {
        beamService.runPipeline();
        return ResponseEntity.ok("Pipeline executed successfully!");
    }
}
