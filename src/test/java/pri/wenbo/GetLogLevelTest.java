package pri.wenbo;

import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Created by twer on 06/12/2016.
 */
public class GetLogLevelTest {
    @Test
    public void shouldReturnNullForEmptyArg() throws IOException {
        GetLogLevel getLogLevel = new GetLogLevel();
        List<String> args = new LinkedList<>();
        args.add("");
        assertThat(getLogLevel.exec(TupleFactory.getInstance().newTuple(args)), nullValue());
    }

    @Test
    public void shouldReturnNullForNullArg() throws IOException {
        GetLogLevel getLogLevel = new GetLogLevel();
        assertThat(getLogLevel.exec(null), nullValue());
        List<String> args = new LinkedList<>();
        assertThat(getLogLevel.exec(TupleFactory.getInstance().newTuple(args)), nullValue());
        args.add(null);
        assertThat(getLogLevel.exec(TupleFactory.getInstance().newTuple(args)), nullValue());
    }

    @Test
    public void shouldReturnCorrectTimeInfo() throws IOException {
        GetLogLevel getLogTime = new GetLogLevel();
        List<String> args = new LinkedList<>();
        args.add("16/12/05 07:28:51 INFO MemoryStore: MemoryStore started with capacity 114.6 MB");
        args.add("yy/MM/dd HH:mm:ss");
        String level = getLogTime.exec(TupleFactory.getInstance().newTuple(args));
        assertThat(level, is("INFO"));
    }
}
