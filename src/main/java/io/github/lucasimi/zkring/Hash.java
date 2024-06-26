package io.github.lucasimi.zkring;

import java.io.Serializable;

import org.apache.commons.codec.digest.MurmurHash3;

public interface Hash {

    <S> int hash(S data);

    public class Default implements Hash {

        record Wrap<S>(S data) implements Serializable {}

        public <S> int hash(S obj) {
            return MurmurHash3.hash32x86(Utils.serialize(new Wrap<>(obj)));
        }

    }

}
