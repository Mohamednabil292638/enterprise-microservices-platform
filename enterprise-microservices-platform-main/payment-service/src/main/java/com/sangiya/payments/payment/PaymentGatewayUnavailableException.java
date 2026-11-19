package com.sangiya.payments.payment;

/** A transient infrastructure failure -- distinct from a business decline, and the only thing @Retry should retry. */
public class PaymentGatewayUnavailableException extends RuntimeException {

    public PaymentGatewayUnavailableException(String message) {
        super(message);
    }
}
