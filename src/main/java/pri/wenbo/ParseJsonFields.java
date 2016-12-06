package pri.wenbo;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.json.JSONObject;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ParseJsonFields extends EvalFunc<Tuple> {
    private final static TupleFactory tupleFactory = TupleFactory.getInstance();

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if(input == null || input.size() < 2)
            return null;
        List<Object> allParam = input.getAll();
        String line = (String) allParam.get(0);
        List<Object> fieldNames = allParam.subList(1, allParam.size());
        List<String> result = new LinkedList<>();
        JSONObject rootJsonObject = new JSONObject(line);
        for (Object fieldName : fieldNames) {
            String[] properties = ((String)fieldName).split("\\.");
            JSONObject jsonObject = rootJsonObject;
            for (int i = 0; i < properties.length - 1; i++) {
                jsonObject = jsonObject.getJSONObject(properties[i]);
            }
            result.add(jsonObject.getString(properties[properties.length - 1]));
        }
        return tupleFactory.newTuple(result);
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.TUPLE));
    }
}
