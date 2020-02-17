package at.schunker.spreadutils;

import java.io.Serializable;
import java.net.InetAddress;

public class SpreadNode implements Serializable {
    protected String name;
    protected InetAddress address;

    public SpreadNode(String name, InetAddress address){
        this.name = name;
        this.address = address;
    }

    public String getName() {
        return this.name;
    }
    public InetAddress getAddress() {
        return this.address;
    }

    public int hashCode(){
        int hc = 0;

        if(this.name != null){
            hc += this.name.hashCode();
        }

        if(this.address != null){
            hc += this.address.hashCode();
        }

        return hc;
    }

    public boolean equals(Object object){
        if(object == null) {
            return false;
        }

        if(!(object instanceof SpreadNode)){
            return false;
        }

        SpreadNode node = (SpreadNode) object;

        if(node.name != null){
            if(!node.name.equals(this.name)){
                return false;
            }
        }

        if(node.address != null){
            if(!node.address.equals(this.address)){
                return false;
            }
        }

        return true;
    }
}
