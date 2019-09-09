package com.wepay.zktools.util;

import java.util.concurrent.CompletableFuture;

public class StateChangeFuture<T> extends CompletableFuture<T> {

    public final T currentState;

    StateChangeFuture(T currentState) {
        this.currentState = currentState;
    }

}
