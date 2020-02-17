package at.schunker.spreadutils;

import java.io.Serializable;

public class SpreadMessageContent implements Serializable {
    public SpreadNode sender;
    public SpreadNode recipient;
    public Serializable data;
}
