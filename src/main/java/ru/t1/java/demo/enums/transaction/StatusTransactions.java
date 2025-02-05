package ru.t1.java.demo.enums.transaction;

public enum StatusTransactions {

    ACCEPTED("ACCEPTED"),
    REJECTED("REJECTED"),
    CANCELLED("CANCELLED"),
    REQUESTED("REQUESTED"),
    BLOCKED("BLOCKED");

    public final String text;

    StatusTransactions(String text){
        this.text = text;
    }
}

