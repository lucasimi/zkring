package io.github.lucasimi.zkring;

import java.io.Serializable;

public record VNode(Node node, int rep) implements Serializable {}