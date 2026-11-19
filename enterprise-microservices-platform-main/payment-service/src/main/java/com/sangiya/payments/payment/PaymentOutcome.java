package com.sangiya.payments.payment;

public record PaymentOutcome(boolean succeeded, String failureReason) {

    public static PaymentOutcome success() {
        return new PaymentOutcome(true, null);
    }

    public static PaymentOutcome declined(String reason) {
        return new PaymentOutcome(false, reason);
    }
}
