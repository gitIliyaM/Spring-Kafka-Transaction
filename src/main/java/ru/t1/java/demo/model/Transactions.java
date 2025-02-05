package ru.t1.java.demo.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import ru.t1.java.demo.enums.transaction.StatusTransactions;

import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "transaction")
public class Transactions {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(name = "idClient", columnDefinition = "INTEGER NOT NULL")
    private Integer idClient;

    @Column(name = "idTransaction", columnDefinition = "INTEGER NOT NULL")
    private Integer idTransaction;

    @Column(name = "amountTransaction", columnDefinition = "DOUBLE PRECISION NOT NULL")
    private Double amountTransaction;

    @Enumerated(value = EnumType.STRING)
    @Column(name = "StatusTransactions", columnDefinition = "TEXT NOT NULL")
    private StatusTransactions statusTransactions;

    @Column(name = "timeTransaction", columnDefinition = "TEXT NOT NULL")
    private String timeTransaction;

    @CreationTimestamp
    @Column(name = "localDateTime")
    private LocalDateTime localDateTime;
}
