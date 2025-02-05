package ru.t1.java.demo.service.postParser;

import ru.t1.java.demo.model.Transactions;
import java.util.List;

public interface PostParsingTransaction {
    List<Transactions> transactionList (String accountPost);
}
