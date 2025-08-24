package com.example.beam.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PersonEnc implements Serializable {
    private long id;
    private String nameEnc;
    private String emailEnc;

}
