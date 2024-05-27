package io.github.lucasimi.zkring;

import java.io.Serializable;
import java.util.UUID;

public record Node(UUID uuid, String address, int port) implements Serializable {}
