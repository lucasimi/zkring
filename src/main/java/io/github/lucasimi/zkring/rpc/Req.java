package io.github.lucasimi.zkring.rpc;

import java.io.Serializable;

public record Req<S>(String service, S data) implements Serializable {}
