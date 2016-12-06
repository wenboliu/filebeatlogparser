package pri.wenbo;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ParseJsonFieldsTest {

    @Test
    public void testExec() throws Exception {
        ParseJsonFields parseJsonFields = new ParseJsonFields();
        List<String> args = new LinkedList<>();
        args.add("{\"@timestamp\":\"2016-12-05T07:28:41.752Z\",\"beat\":{\"hostname\":\"for-ambari\",\"name\":\"for-ambari\",\"version\":\"5.0.2\"},\"input_type\":\"log\",\"message\":\"16/12/05 07:28:39 INFO SecurityManager: Changing view acls to: yarn,spark\",\"offset\":1544,\"source\":\"/hadoop/yarn/log/application_1480666645624_0027/container_1480666645624_0027_01_000001/stderr\",\"type\":\"log\"}");
        args.add("source");
        args.add("message");
        Tuple result = parseJsonFields.exec(TupleFactory.getInstance().newTuple(args));
        List<Object> all = result.getAll();
        assertThat(all.size(), is(2));
        assertThat(all.get(0), is("/hadoop/yarn/log/application_1480666645624_0027/container_1480666645624_0027_01_000001/stderr"));
        assertThat(all.get(1), is("16/12/05 07:28:39 INFO SecurityManager: Changing view acls to: yarn,spark"));
    }
}