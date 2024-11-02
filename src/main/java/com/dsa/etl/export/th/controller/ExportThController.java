//package com.dsa.etl.export.th.controller;
//
//import com.dsa.etl.export.th.model.entities.ExportThEntity;
//import com.dsa.etl.export.th.model.entities.ExportThId;
//import com.dsa.etl.export.th.repository.ExportThRepository;
//import lombok.RequiredArgsConstructor;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.*;
//
//import java.util.List;
//
//@RestController
//@RequestMapping("/api/export-th")
//@RequiredArgsConstructor
//public class ExportThController {
//    private final ExportThRepository repository;
//
//    @GetMapping
//    public ResponseEntity<List<ExportThEntity>> getAll() {
//        return ResponseEntity.ok(repository.findAll());
//    }
//
//    @GetMapping("/find")
//    public ResponseEntity<ExportThEntity> findById(
//            @RequestParam String country,
//            @RequestParam Integer hs2dg,
//            @RequestParam Integer hs4dg,
//            @RequestParam String month,
//            @RequestParam String year) {
//        ExportThId id = new ExportThId(country, hs2dg, hs4dg, month, year);
//        return repository.findById(id)
//                .map(ResponseEntity::ok)
//                .orElse(ResponseEntity.notFound().build());
//    }
//
//    @GetMapping("/search")
//    public ResponseEntity<List<ExportThEntity>> search(
//            @RequestParam(required = false) String country,
//            @RequestParam(required = false) Integer hs2dg) {
//        if (country != null && hs2dg != null) {
//            return ResponseEntity.ok(repository.findByCountryAndHs2dg(country, hs2dg));
//        } else if (country != null) {
//            return ResponseEntity.ok(repository.findByCountry(country));
//        } else if (hs2dg != null) {
//            return ResponseEntity.ok(repository.findByHs2dg(hs2dg));
//        }
//        return ResponseEntity.ok(repository.findAll());
//    }
//
//    @PostMapping
//    public ResponseEntity<ExportThEntity> create(@RequestBody ExportThId id) {
//        ExportThEntity entity = new ExportThEntity();
//        entity.setId(id);
//        return ResponseEntity.ok(repository.save(entity));
//    }
//
//    @DeleteMapping
//    public ResponseEntity<Void> delete(
//            @RequestParam String country,
//            @RequestParam Integer hs2dg,
//            @RequestParam Integer hs4dg,
//            @RequestParam String month,
//            @RequestParam String year) {
//        ExportThId id = new ExportThId(country, hs2dg, hs4dg, month, year);
//        if (!repository.existsById(id)) {
//            return ResponseEntity.notFound().build();
//        }
//        repository.deleteById(id);
//        return ResponseEntity.noContent().build();
//    }
//}
