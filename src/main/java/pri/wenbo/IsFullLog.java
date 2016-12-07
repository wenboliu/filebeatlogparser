package pri.wenbo;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Created by twer on 07/12/2016.
 */
public class IsFullLog extends FilterFunc {
    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        if(tuple == null || tuple.size() != 2) return false;
        String log = (String) tuple.get(0);
        if(log == null || "".equals(log.trim())) return false;
        String format = (String) tuple.get(1);
        try {
            new SimpleDateFormat(format).parse(log).getTime();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
