package ru.t1.java.demo.aop;

import java.lang.annotation.ElementType;
import java.lang.annotation.*;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LogException {
}
