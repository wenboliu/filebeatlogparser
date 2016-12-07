package pri.wenbo;

import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class IsFullLogTest {
    @Test
    public void shouldReturnTrueForFullLog() throws IOException {
        IsFullLog isFullLog = new IsFullLog();
        List<String> args = new LinkedList<>();
        args.add("16/12/05 07:28:51 INFO MemoryStore: MemoryStore started with capacity 114.6 MB");
        args.add("yy/MM/dd HH:mm:ss");
        assertThat(isFullLog.exec(TupleFactory.getInstance().newTuple(args)), is(true));
    }

    @Test
    public void shouldReturnFalseForNoneFullLog() throws IOException {
        IsFullLog isFullLog = new IsFullLog();
        List<String> args = new LinkedList<>();
        args.add("============================================");
        args.add("yy/MM/dd HH:mm:ss");
        assertThat(isFullLog.exec(TupleFactory.getInstance().newTuple(args)), is(false));
    }

}