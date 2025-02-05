package ru.t1.java.demo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import ru.t1.java.demo.enums.account.StatusAccounts;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import ru.t1.java.demo.enums.account.TypeAccount;
import java.util.Random;

@Getter
@Setter
public class AccountDTO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JsonProperty()
    @NotNull(message = "idClient cannot be null")
    private Integer idClient;

    @JsonProperty()
    @NotNull(message = "idAccount cannot be null")
    private Integer idAccount;

    @JsonProperty()
    @NotBlank(message = "accountType cannot be blank")
    private TypeAccount typeAccount;

    @Enumerated(value = EnumType.STRING)
    @NotBlank(message = "statusAccounts cannot be blank")
    @JsonProperty()
    private StatusAccounts statusAccounts;

    @JsonProperty()
    @NotNull(message = "balance cannot be null")
    private Double balance;

    @JsonProperty()
    @NotNull(message = "frozenAmount cannot be null")
    private Double frozenAmount;

    public Long getId() {
        long newLong =  new Random().nextLong(Long.MAX_VALUE) + 1;
        if(newLong < 0){
           return -newLong;
        } else {
            return newLong;
        }
    }
}
