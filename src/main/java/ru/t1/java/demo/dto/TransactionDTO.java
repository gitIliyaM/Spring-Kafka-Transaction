package ru.t1.java.demo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import ru.t1.java.demo.enums.transaction.StatusTransactions;
import java.time.LocalDateTime;
import java.util.Random;

@Getter
@Setter
public class TransactionDTO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JsonProperty()
    @NotNull(message = "idClient cannot be null")
    private Integer idClient;

    @JsonProperty()
    @NotNull(message = "idTransaction cannot be null")
    private Integer idTransaction;

    @JsonProperty()
    @NotNull(message = "amountTransaction cannot be null")
    private Double amountTransaction;

    @Enumerated(value = EnumType.STRING)
    @JsonProperty()
    @NotBlank(message = "statusTransactions cannot be blank")
    private StatusTransactions statusTransactions;

    @JsonProperty()
    @NotBlank(message = "timeTransaction cannot be blank")
    private String timeTransaction;

    @CreationTimestamp
    @JsonProperty()
    private LocalDateTime localDateTime;

    public Long getId() {
        long newLong =  new Random().nextLong(Long.MAX_VALUE) + 1;
        if(newLong < 0){
            return -newLong;
        } else {
            return newLong;
        }
    }
}
