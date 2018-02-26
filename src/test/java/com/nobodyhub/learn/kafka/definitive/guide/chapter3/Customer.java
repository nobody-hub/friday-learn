package com.nobodyhub.learn.kafka.definitive.guide.chapter3;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Ryan
 */
@AllArgsConstructor
@Getter
public class Customer {
    private int customerId;
    private String customerName;
}
