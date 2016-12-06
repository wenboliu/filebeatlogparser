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
public class GetLogClassTest {
    @Test
    public void shouldReturnZeroForNullArg() throws IOException {
        GetLogClass getLogTime = new GetLogClass();

        assertThat(getLogTime.exec(null), nullValue());
        assertThat(getLogTime.exec(TupleFactory.getInstance().newTuple(new LinkedList<>())), nullValue());
    }

    @Test
    public void shouldReturnZeroForEmptyArg() throws IOException {
        GetLogClass getLogTime = new GetLogClass();
        List<String> args = new LinkedList<>();
        args.add("");
        String clazz = getLogTime.exec(TupleFactory.getInstance().newTuple(args));
        assertThat(clazz, nullValue());
    }

    @Test
    public void shouldReturnCorrectTimeInfo() throws IOException {
        GetLogClass getLogTime = new GetLogClass();
        List<String> args = new LinkedList<>();
        args.add("16/12/05 07:28:51 INFO MemoryStore: MemoryStore started with capacity 114.6 MB");
        args.add("yy/MM/dd HH:mm:ss");
        String clazz = getLogTime.exec(TupleFactory.getInstance().newTuple(args));
        assertThat(clazz, is("MemoryStore"));
    }
}
