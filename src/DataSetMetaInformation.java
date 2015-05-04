/**
 * Created by Bence on 2015.04.25..
 */
public class DataSetMetaInformation {
    //The dataset fullname, you can use it in a query
    //for example: select * from dmi.name
    public final String name;

    //The sql query's needed attributes
    public final String[] attributes;

    public DataSetMetaInformation(final String _name, String[] _attributes) {
        name = _name;
        attributes = _attributes;
    }
}