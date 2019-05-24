package lvliang;

import java.io.Serializable;

/**
 * <p>Company:misspao </p >
 *
 * @author: lvliang
 * @Date: Create in 10:40 2019-05-24
 * @Description:
 */
public class Word implements Serializable {

    public void say(String s) {
        System.out.println("------" + s);
    }
}
