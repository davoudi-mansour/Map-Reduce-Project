

import java.util.Comparator;

class Url {

    int key;
    String value;
    public Url(int key,String value){
        this.key=key;
        this.value=value;
    }

    public int getKey(){return this.key;}
    public String getValue(){return this.value;}
}


class url_ip {

    int key;
    String value;
    public url_ip(int key,String value){
        this.key=key;
        this.value=value;
    }

    public int getKey(){return this.key;}
    public String getValue(){return this.value;}

}
class url_ipComparator implements Comparator<url_ip> {

    public int compare(url_ip t1, url_ip t2) {

        return t2.key - t1.key;
    }

}

class urlComparator implements Comparator<Url> {

    public int compare(Url t1, Url t2) {

        return t2.key - t1.key;
    }

}

