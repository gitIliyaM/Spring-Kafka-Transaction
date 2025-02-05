package ru.t1.java.demo.service.postParser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import ru.t1.java.demo.model.Accounts;
import java.io.IOException;
import java.util.List;

@Component
public class PostStringAccount implements PostParsingAccount {

    @Override
    public List<Accounts> accountList (String accountPost)  {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(accountPost, objectMapper.getTypeFactory().constructCollectionType(List.class, Accounts.class));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
