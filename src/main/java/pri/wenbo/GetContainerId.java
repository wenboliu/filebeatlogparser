package pri.wenbo;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;


public class GetContainerId extends EvalFunc<String> {
    @Override
    public String exec(Tuple tuple) throws IOException {
        if(tuple == null || tuple.size() != 1) return null;
        String source = (String) tuple.get(0);
        if(source == null) return null;
        String[] paths = source.split("\\/");
        for (String path : paths) {
            if(path.contains("container_")) return path.replace("container_", "");
        }
        return null;
    }
}
