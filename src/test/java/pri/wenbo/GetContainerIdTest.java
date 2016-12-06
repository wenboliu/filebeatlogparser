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
public class GetContainerIdTest {
    @Test
    public void shouldGetCorrectContainerId() throws IOException {
        GetContainerId getContainerId = new GetContainerId();
        List<String> args = new LinkedList<>();
        args.add("/hadoop/yarn/log/application_1480666645624_0027/container_1480666645624_0027_01_000001/stderr");
        String applicationId = getContainerId.exec(TupleFactory.getInstance().newTuple(args));

        assertThat(applicationId, is("1480666645624_0027_01_000001"));
    }

    @Test
    public void shouldReturnNullForEmptyArg() throws IOException {
        GetContainerId getContainerId = new GetContainerId();
        List<String> args = new LinkedList<>();
        args.add("");
        assertThat(getContainerId.exec(TupleFactory.getInstance().newTuple(args)), nullValue());
    }

    @Test
    public void shouldReturnNullForNullArg() throws IOException {
        GetContainerId getContainerId = new GetContainerId();
        assertThat(getContainerId.exec(null), nullValue());
        List<String> args = new LinkedList<>();
        assertThat(getContainerId.exec(TupleFactory.getInstance().newTuple(args)), nullValue());
        args.add(null);
        assertThat(getContainerId.exec(TupleFactory.getInstance().newTuple(args)), nullValue());
    }
}
