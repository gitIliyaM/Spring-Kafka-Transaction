package ru.t1.java.demo.service.postParser;

import ru.t1.java.demo.model.Accounts;
import java.util.List;

public interface PostParsingAccount {
    List<Accounts> accountList (String accountPost);
}
