package at.schunker.spreadutils;

import spread.SpreadGroup;

public interface MembershipMessageListener {
    public void membershipInfo(SpreadGroup[] groups);
    public void membershipJoined(SpreadGroup group);
    public void membershipLeft(SpreadGroup group);
    public void membershipDisconnected(SpreadGroup group);
    public void membershipNetwork(SpreadGroup[] group);
}
