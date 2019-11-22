import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HelloHbase {

    private TableName table1 = TableName.valueOf("Table22");
    private String family1 = "Family1";
    private String family2 = "Family2";

    public static void main(String[] args) throws IOException {
        HelloHbase helloHbase = new HelloHbase();
        Configuration config = helloHbase.getConfig();

        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        ColumnFamilyDescriptor columnFamilyDescriptor1 = ColumnFamilyDescriptorBuilder.of(helloHbase.family1);
        ColumnFamilyDescriptor columnFamilyDescriptor2 = ColumnFamilyDescriptorBuilder.of(helloHbase.family2);
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(helloHbase.table1);
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor1);
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor2);

        TableDescriptor descriptor = tableDescriptorBuilder.build();
        admin.createTable(descriptor);
    }

    public Configuration getConfig() {
        Configuration config = HBaseConfiguration.create();
        String path = HelloHbase.class.getResource("hbase-site.xml").getPath();
        config.addResource(new Path(path));

        return config;
    }
}
