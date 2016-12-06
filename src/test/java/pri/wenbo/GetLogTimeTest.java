package pri.wenbo;

import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by twer on 06/12/2016.
 */
public class GetLogTimeTest {
    @Test
    public void shouldReturnZeroForNullArg() throws IOException {
        GetLogTime getLogTime = new GetLogTime();

        assertThat(getLogTime.exec(null), is(0L));
        assertThat(getLogTime.exec(TupleFactory.getInstance().newTuple(new LinkedList<>())), is(0L));
    }

    @Test
    public void shouldReturnZeroForEmptyArg() throws IOException {
        GetLogTime getLogTime = new GetLogTime();
        List<String> args = new LinkedList<>();
        args.add("");
        Long time = getLogTime.exec(TupleFactory.getInstance().newTuple(args));
        assertThat(time, is(0L));
    }

    @Test
    public void shouldReturnCorrectTimeInfo() throws IOException {
        GetLogTime getLogTime = new GetLogTime();
        List<String> args = new LinkedList<>();
        args.add("16/12/05 07:28:51 INFO MemoryStore: MemoryStore started with capacity 114.6 MB");
        args.add("yy/MM/dd HH:mm:ss");
        Long time = getLogTime.exec(TupleFactory.getInstance().newTuple(args));
        assertThat(time, is(1480894131000L));
    }
}
