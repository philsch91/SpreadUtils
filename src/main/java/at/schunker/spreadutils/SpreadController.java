/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package at.schunker.spreadutils;

import spread.*;
import spread.MembershipInfo.VirtualSynchronySet;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SpreadController implements AdvancedMessageListener {
    
    protected final SpreadConnection connection;
    protected String name;
    protected InetAddress address;
    protected CopyOnWriteArrayList<SpreadNode> nodeList = new CopyOnWriteArrayList<>();
    protected CopyOnWriteArrayList<SpreadGroup> groupList = new CopyOnWriteArrayList<>();
    protected ConcurrentHashMap<String, ArrayList<AdvancedMessageListener>> listeners = new ConcurrentHashMap<>();
    protected ConcurrentHashMap<String, ArrayList<MembershipMessageListener>> membershipListeners = new ConcurrentHashMap<>();
    private boolean selfDiscard = false;
    private boolean passiveReplication = false;

    public SpreadController(String dnsname, String name, int port) throws UnknownHostException, SpreadException {
        this.connection = new SpreadConnection();
        this.address = InetAddress.getByName(dnsname);
        this.name = name;

        //System.out.println("using address " + this.address.toString() +  " for Spread");

        //take care that you set the last parameter of SpreadConnection.connect() to true
        //otherwise, you will not get any membership notifications
        this.connection.add(this);
        this.connection.connect(this.address, port, this.name, false, true);
    }

    public SpreadController(String dnsname, String name) throws UnknownHostException, SpreadException {
        this(dnsname, name,4803);
    }

    public List<SpreadNode> getNodeList() {
        return this.nodeList;
    }

    public boolean isSelfDiscard() {
        return this.selfDiscard;
    }

    public void setSelfDiscard(boolean selfDiscard) {
        this.selfDiscard = selfDiscard;
    }

    public boolean isPassiveReplication() {
        return this.passiveReplication;
    }

    public void setPassiveReplication(boolean passiveReplication) {
        this.passiveReplication = passiveReplication;
        this.sendSpreadReplicationMessageIfNeeded();
    }

    public boolean isPrimary() {
        List<String> nameList = new ArrayList<>();
        for(SpreadNode node : this.nodeList){
            nameList.add(node.name);
        }

        Collections.sort(nameList);

        String firstName = nameList.get(0);
        if(!this.name.contains(firstName)){
            return false;
        }
        return true;
    }

    /**
     * Joins the group with the given name on the connection and
     * all messages sent to the group will be received by the connection
     * @param groupname
     * @return boolean
     * @throws SpreadException
     */
    protected boolean joinGroup(String groupname) throws SpreadException {
        //String groupname = group.name();
        for(SpreadGroup spreadGroup : this.groupList){
            if(spreadGroup.toString().equals(groupname)){
                System.out.println("SpreadController already joined in " + groupname + " group");
                return false;
            }
        }

        SpreadGroup spreadGroup = new SpreadGroup();
        spreadGroup.join(this.connection, groupname);

        this.groupList.add(spreadGroup);
        return true;
    }

    /**
     * Leaves the group with the given name and
     * no new messages sent to this group are received anymore
     * @param groupname
     * @return boolean
     * @throws SpreadException
     */
    protected boolean leaveGroup(String groupname) throws SpreadException {
        //String groupname = group.name();
        SpreadGroup spreadGroup = null;

        for(SpreadGroup sgroup : this.groupList){
            if(sgroup.toString().equals(groupname)){
                spreadGroup = sgroup;
            }
        }

        if(spreadGroup == null){
            System.out.println("SpreadController not joined in " + groupname + " group");
            return false;
        }

        spreadGroup.leave();

        this.groupList.remove(spreadGroup);
        return true;
    }

    public boolean addMessageListener(AdvancedMessageListener listener, String groupname) throws SpreadException {
        boolean isJoined = this.joinGroup(groupname);

        //if(!isJoined){ return false; }
        //String groupName = group.name();

        ArrayList<AdvancedMessageListener> listenerList;

        if(this.listeners.containsKey(groupname)){
            System.out.println("Listener already registered for " + groupname + " group");
            listenerList = this.listeners.get(groupname);
            //return false;
        } else {
            listenerList = new ArrayList<>(1);
            listenerList.add(listener);
        }

        this.listeners.put(groupname, listenerList);
        return true;
    }

    public boolean removeMessageListener(AdvancedMessageListener listener, String groupname) throws SpreadException {
        boolean hasLeft = this.leaveGroup(groupname);

        if(!hasLeft){
            return false;
        }

        //String groupName = group.name();

        if(!this.listeners.containsKey(groupname)){
            System.out.println("Listener not registered in " + groupname + " group");
            return false;
        }

        ArrayList listenerList = this.listeners.get(groupname);
        listenerList.remove(listener);

        if(listenerList.size() == 0){
            this.listeners.remove(groupname);
        }

        return true;
    }

    public boolean addMembershipMessageListener(MembershipMessageListener listener, String groupname) throws SpreadException {
        boolean isJoined = this.joinGroup(groupname);

        ArrayList<MembershipMessageListener> listenerList;

        if(this.membershipListeners.containsKey(groupname)){
            System.out.println("Listener already registered for " + groupname + " group");
            listenerList = this.membershipListeners.get(groupname);
            //return false;
        } else {
            listenerList = new ArrayList<>(1);
            listenerList.add(listener);
        }

        this.membershipListeners.put(groupname, listenerList);
        return true;
    }

    public boolean removeMembershipMessageListener(MembershipMessageListener listener, String groupname) throws SpreadException {
        boolean hasLeft = this.leaveGroup(groupname);

        if(!hasLeft){
            return false;
        }

        if(!this.membershipListeners.containsKey(groupname)){
            System.out.println("Listener not registered in " + groupname + " group");
            return false;
        }

        ArrayList listenerList = this.membershipListeners.get(groupname);
        listenerList.remove(listener);

        if(listenerList.size() == 0){
            this.listeners.remove(groupname);
        }

        return true;
    }

    @Override
    public void membershipMessageReceived(SpreadMessage spreadMessage) {
        MembershipInfo membershipInfo = spreadMessage.getMembershipInfo();
        SpreadGroup[] spreadGroups = spreadMessage.getGroups();

        SpreadGroup membershipGroup = membershipInfo.getGroup();
        //System.out.println("membership group is " + membershipGroup.toString());

        if(membershipInfo.isRegularMembership()){
            //System.out.println("regular membership message received");
            SpreadGroup[] groups = membershipInfo.getMembers();
            /*
            System.out.println("member count: " + Integer.toString(groups.length));
            if(groups.length > 0){
                System.out.println("members:");
            }

            for(SpreadGroup group : groups){
                System.out.println(group.toString());
            } */

            //this.sendSpreadReplicationMessageIfNeeded();

            //TODO: sync with this.node

            if(this.membershipListeners.containsKey(membershipGroup.toString())){
                for(MembershipMessageListener listener : this.membershipListeners.get(membershipGroup.toString())){
                    listener.membershipInfo(groups);
                }
            }
        }

        if(membershipInfo.isCausedByJoin()){
            //System.out.println("membership message caused by join received");
            SpreadGroup group = membershipInfo.getJoined();
            String spreadName = group.toString();

            System.out.println(spreadName + " joined");
            spreadName = this.extractSpreadName(spreadName);

            SpreadNode node = new SpreadNode(spreadName, null);
            this.addSpreadNode(node);

            if(this.nodeList.size() > 0){
                System.out.println("nodelist: ");
            }

            for(SpreadNode snode : this.nodeList){
                System.out.println(snode.getName());
            }

            if(this.name.contains(spreadName)){
                //System.out.println(this.name + " contains " + spreadName);
                System.out.println("self discard SpreadMessage");
                return;
            }

            if(this.membershipListeners.containsKey(membershipGroup.toString())){
                for(MembershipMessageListener listener : this.membershipListeners.get(membershipGroup.toString())){
                    listener.membershipJoined(group);
                }
            }
        } else if(membershipInfo.isCausedByLeave()){
            //System.out.println("membership message caused by leave received");
            SpreadGroup group = membershipInfo.getLeft();
            String spreadName = group.toString();

            spreadName = this.extractSpreadName(spreadName);

            SpreadNode node = new SpreadNode(spreadName, null);
            this.removeSpreadNode(node);

            System.out.println("nodelist: ");
            for(SpreadNode snode : this.nodeList){
                System.out.println(snode.getName());
            }

            //System.out.println(spreadName + " left");

            if(this.membershipListeners.containsKey(membershipGroup.toString())){
                for(MembershipMessageListener listener : this.membershipListeners.get(membershipGroup.toString())){
                    listener.membershipLeft(group);
                }
            }
        } else if(membershipInfo.isCausedByDisconnect()){
            //System.out.println("membership message caused by disconnect received");
            SpreadGroup group = membershipInfo.getDisconnected();
            String spreadName = group.toString();

            spreadName = this.extractSpreadName(spreadName);

            SpreadNode node = new SpreadNode(spreadName, null);
            this.removeSpreadNode(node);

            //System.out.println(spreadName + " disconnected");

            if(this.membershipListeners.containsKey(membershipGroup.toString())){
                for(MembershipMessageListener listener : this.membershipListeners.get(membershipGroup.toString())){
                    listener.membershipDisconnected(group);
                }
            }
        } else if(membershipInfo.isCausedByNetwork()){
            //memberShipInfo.getStayed() is deprecated
            //System.out.println("membership message caused by network received");

            VirtualSynchronySet[] virtSyncSets = membershipInfo.getVirtualSynchronySets();
            System.out.println("virtSyncSet.length: " + Integer.toString(virtSyncSets.length));
            VirtualSynchronySet set = virtSyncSets[0];
            SpreadGroup[] members = set.getMembers();

            //TODO: check for name or dnsname
            for (SpreadGroup g : members) {
                System.out.println("SpreadGroup as member: " + g.toString());
            }

            SpreadNode missingNode = null;
            //compare saved nodes with members

            for (SpreadNode node : this.nodeList) {
                boolean isMissing = true;
                for(SpreadGroup group : members){
                    String spreadName = this.extractSpreadName(group.toString());
                    if(spreadName.equals(node.name)){
                        isMissing = false;
                    }
                }

                if(isMissing){
                    missingNode = node;
                    System.out.println("node " + missingNode.getName() + " is missing");
                }
            }

            if(this.membershipListeners.containsKey(membershipGroup.toString())){
                for(MembershipMessageListener listener : this.membershipListeners.get(membershipGroup.toString())){
                    listener.membershipNetwork(members);
                }
            }
        }

        for(SpreadGroup group : spreadGroups){
            if(this.listeners.containsKey(group.toString())){
                for(AdvancedMessageListener listener : this.listeners.get(group.toString())){
                    listener.membershipMessageReceived(spreadMessage);
                }
            }
        }
    }

    @Override
    public void regularMessageReceived(SpreadMessage spreadMessage) {
        System.out.println("regulardMessageReceived");
        SpreadGroup[] spreadGroups = spreadMessage.getGroups();
        Object object = null;

        try {
            object = spreadMessage.getObject();
        } catch (SpreadException e) {
            e.printStackTrace();
        }

        if(object != null && object instanceof SpreadMessageContent){
            SpreadMessageContent content = (SpreadMessageContent) object;
            this.saveSpreadNodeIfNeeded(content);

            if(content.sender != null && content.sender.equals(new SpreadNode(this.name, this.address))){
                return;
            }
        }

        for(SpreadGroup group : spreadGroups){
            if(this.listeners.containsKey(group.toString())){
                for(AdvancedMessageListener listener : this.listeners.get(group.toString())){
                    listener.regularMessageReceived(spreadMessage);
                }
            }
        }
    }

    public boolean sendSpreadMessage(SpreadMessage message) throws SpreadException {
        Object obj = message.getObject();
        message.setSelfDiscard(this.selfDiscard);

        if(!(obj instanceof SpreadMessageContent)){
            this.connection.multicast(message);
            return true;
        }

        SpreadMessageContent payload = (SpreadMessageContent) obj;
        payload.sender = new SpreadNode(this.name, this.address);
        message.setObject(payload);

        //override selfDiscard for saving the own SpreadNode in this.sp
        if(payload.data.equals(this.getClass().getSimpleName())){
            message.setSelfDiscard(false);
        }

        this.connection.multicast(message);

        return true;
    }

    /**
     * sendSpreadReplicationMessageIfNeeded extends sendSpreadMessage
     * if this.passiveReplication is true
     * @return
     */
    public boolean sendSpreadReplicationMessageIfNeeded(){
        if(!this.isPassiveReplication()){
            return false;
        }

        //inform the other replicas about this existing replica
        List<String> groups = new ArrayList<String>();
        for(SpreadGroup grp : this.groupList){
            groups.add(grp.toString());
        }

        SpreadMessage message = new SpreadMessage();
        message.setReliable();
        message.addGroups(groups.toArray(new String[0]));

        SpreadMessageContent content = new SpreadMessageContent();
        content.data = this.getClass().getSimpleName();

        try {
            message.setObject(content);
            this.sendSpreadMessage(message);
            System.out.println("sendSpreadReplicationMessage");
        } catch (SpreadException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    private String extractSpreadName(String name){
        if(name.contains("#") && name.indexOf("#") != name.lastIndexOf("#")){
            name = name.substring(name.indexOf("#")+1, name.lastIndexOf("#"));
        }
        //System.out.println("name is " + this.name);
        //System.out.println("name is " + name);
        return name;
    }

    private boolean addSpreadNode(SpreadNode node){
        if(this.nodeList.contains(node)){
            return false;
        }

        this.nodeList.add(node);
        return true;
    }

    /**
     * saveSpreadNodeIfNeeded extends addSpreadNode
     * if this.passiveReplication is true
     * @param content
     * @return
     */
    private boolean saveSpreadNodeIfNeeded(SpreadMessageContent content){
        System.out.println("saveSpreadNodeIfNeeded");
        if(!this.isPassiveReplication()){
            return false;
        }

        if(content.data != this.getClass().getSimpleName()){
            return false;
        }

        this.addSpreadNode(content.sender);

        return true;
    }

    private boolean removeSpreadNode(SpreadNode node){
        return this.nodeList.remove(node);
    }
}
