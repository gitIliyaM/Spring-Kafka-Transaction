package ru.t1.java.demo.model;

import jakarta.persistence.*;
import lombok.*;

@Getter
@Setter
@Entity
@Table(name = "Errors")
public class DataSourceErrorLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Lob
    @Column()
    private String exceptionStackTrace;

    @Column(length = 1024)
    private String message;

    @Column(length = 1024)
    private String signatureMethod;

}
