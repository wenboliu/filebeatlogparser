package pri.wenbo;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Created by twer on 06/12/2016.
 */
public class GetLogTime extends EvalFunc<Long> {
    @Override
    public Long exec(Tuple tuple) throws IOException {
        if(tuple == null || tuple.size() != 2) return 0L;
        String log = (String) tuple.get(0);
        if(log == null || "".equals(log.trim())) return 0L;
        String format = (String) tuple.get(1);
        try {
            return new SimpleDateFormat(format).parse(log).getTime();
        } catch (Exception e) {
            return 0L;
        }
    }
}
