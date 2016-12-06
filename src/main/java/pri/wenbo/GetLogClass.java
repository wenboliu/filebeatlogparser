package pri.wenbo;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by twer on 06/12/2016.
 */
public class GetLogClass extends EvalFunc<String> {
    @Override
    public String exec(Tuple tuple) throws IOException {
        if(tuple == null || tuple.size() != 2) return null;
        String log = (String) tuple.get(0);
        if(log == null || "".equals(log.trim())) return null;
        String format = (String) tuple.get(1);
        DateFormat simpleDateFormat = new SimpleDateFormat(format);
        try {
            String logExceptTime = log.replace(simpleDateFormat.format(simpleDateFormat.parse(log)) + " ", "");
            return logExceptTime.split("\\s")[1].replace(":","");
        } catch (Exception e) {
            return null;
        }
    }
}
