package MapReduce;

import org.apache.hadoop.util.StringUtils;


/**
 *
 */
public class Node {
    private String distance;
    private String[] adjs;


    public String getDistance() {
        return distance;
    }


    public void setDistance(String distance) {
        this.distance = distance;
    }

    public String getKey(String str)
    {
        return str.substring(1, str.indexOf(","));
    }

    public String getValue(String str)
    {
        return str.substring(str.indexOf(",")+1, str.indexOf(")"));
    }

    public String getNodeKey(int num)
    {
        return getKey(adjs[num]);
    }

    public String getNodeValue(int num)
    {
        return getValue(adjs[num]);
    }

    public int getNodeNum()
    {
        return adjs.length;
    }

    public void FormatNode(String str)
    {
        if(str.length() == 0)
            return ;

        String[] strs =  StringUtils.split(str, ' ');

        adjs = new String[strs.length-1];
        for(int i=0; i<strs.length; i++)
        {
            if(i == 0)
            {
                setDistance(strs[i]);
                continue;
            }
            this.adjs[i-1]=strs[i];
        }
    }

    public String toString()
    {
        String str = this.distance+"" ;

        if(this.adjs == null)
            return str;

        for(String s:this.adjs)
        {
            str = str+" "+s;
        }
        return str;
    }

    public static void main(String[] args)
    {
        Node node  = new Node();
        node.FormatNode("1    (A,20)    (B,30)");
        System.out.println(node.distance+"|"+node.getNodeNum()+"|"+node.toString());
    }
}
