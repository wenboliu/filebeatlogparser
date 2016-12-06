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
public class GetApplicationIdTest {
    @Test
    public void shouldGetCorrectApplicationId() throws IOException {
        GetApplicationId getApplicationId = new GetApplicationId();
        List<String> args = new LinkedList<>();
        args.add("/hadoop/yarn/log/application_1480666645624_0027/container_1480666645624_0027_01_000001/stderr");
        String applicationId = getApplicationId.exec(TupleFactory.getInstance().newTuple(args));

        assertThat(applicationId, is("1480666645624_0027"));
    }

    @Test
    public void shouldReturnNullForEmptyArg() throws IOException {
        GetApplicationId getApplicationId = new GetApplicationId();
        List<String> args = new LinkedList<>();
        args.add("");
        assertThat(getApplicationId.exec(TupleFactory.getInstance().newTuple(args)), nullValue());
    }

    @Test
    public void shouldReturnNullForNullArg() throws IOException {
        GetApplicationId getApplicationId = new GetApplicationId();
        assertThat(getApplicationId.exec(null), nullValue());
        List<String> args = new LinkedList<>();
        assertThat(getApplicationId.exec(TupleFactory.getInstance().newTuple(args)), nullValue());
        args.add(null);
        assertThat(getApplicationId.exec(TupleFactory.getInstance().newTuple(args)), nullValue());
    }
}
